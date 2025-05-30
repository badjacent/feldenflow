from typing import List, Dict, Optional, Literal
import asyncio
import os
import json
import httpx
from datetime import datetime, timezone
import transcription_utils

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

from dotenv import load_dotenv
load_dotenv()

# Import our database models
from database import (
    SourceType, 
    DocumentProvider, 
    YTVideo,
    YTPrivateChannelVideo,
    AudioFile,
    Transcription,
    GroqJob,
    GroqJobChunk,
    GroqJobStatus,
    get_sources_ready_for_processing,
    update_job_status,
    get_source_file_info
)

from groq_utils import (
    create_groq_jsonl_file,
    send_groq_batch,
    retrieve_groq_job_status,
    update_groq_job_chunks
)
from anthropic_utils import merge_multiple_transcriptions

@task(retries=2, retry_delay_seconds=10)
async def submit_groq_job(job: Dict) -> Dict:
    """
    Submit a job to Groq for transcription
    
    Args:
        job: Job dictionary with batch information
        api_key: Groq API key
        
    Returns:
        Updated job dictionary with Groq request information
    """
    logger = get_run_logger()
    
    logger.info(f"Submitting job {job['job_id']} with {len(job['chunks'])} chunks to Groq")

    jsonl_file_path = create_groq_jsonl_file(job["chunks"])
    job = send_groq_batch(job["id"], jsonl_file_path)
    
    return job

@task
async def check_groq_job_status(job_id: int) -> Dict:
    """
    Check the status of a Groq job
    
    Args:
        job_id: ID of the Groq job in our database
        
    Returns:
        Updated job status dictionary
    """
    logger = get_run_logger()
    api_key = os.getenv("GROQ_API_KEY")
    
    # Get job details from database
    groq_job = GroqJob.get_by_id(job_id)
    if not groq_job:
        logger.error(f"Job with ID {job_id} not found in database")
        return {"id": job_id, "status": "error", "error": "Job not found"}
    
    # Skip if job is not in submitted status
    if groq_job.status != "submitted":
        logger.info(f"Job {groq_job.job_id} has status {groq_job.status}, skipping status check")
        return {"id": job_id, "job_id": groq_job.job_id, "status": groq_job.status}
    
    # Check status with Groq API
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(
                f"https://api.groq.com/v1/audio/transcriptions/batch/{groq_job.groq_job_id}",
                headers={
                    "Authorization": f"Bearer {api_key}"
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to check status of job {groq_job.groq_job_id}: {response.text}")
                # Update job with error status
                update_job_status(
                    groq_job, 
                    status="error", 
                    error=response.text
                )
                return {
                    "id": job_id, 
                    "job_id": groq_job.job_id, 
                    "status": "error", 
                    "error": response.text
                }
            
            status_data = response.json()
            job_status = status_data.get("status")
            
            logger.info(f"Job {groq_job.job_id} (Groq job ID: {groq_job.groq_job_id}) has status: {job_status}")
            
            # Update database based on status
            if job_status == "completed":
                # Get the results
                results = status_data.get("results", [])
                
                # Get all chunks for this job
                chunks = GroqJobChunk.get_by_job_id(job_id)
                chunk_mappings = {
                    os.path.basename(chunk.s3_uri): chunk
                    for chunk in chunks if chunk.s3_uri
                }
                
                # Update each result
                for result in results:
                    file_id = result.get("file_id")
                    transcription = result.get("text", "")
                    
                    # Match by file name or other identifiers from Groq
                    for file_name, chunk in chunk_mappings.items():
                        if f"placeholder_file_id_for_{file_name}" == file_id:
                            # Update chunk with transcription
                            chunk.transcription = transcription
                            chunk.updated_at = datetime.now(timezone.utc)
                            chunk.save()
                            break
                
                # Update job status
                update_job_status(
                    groq_job,
                    status="completed",
                    completed_at=datetime.now(timezone.utc)
                )
                
                logger.info(f"Updated job {groq_job.job_id} status to completed")
                
                return {
                    "id": job_id,
                    "job_id": groq_job.job_id,
                    "status": "completed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "results_count": len(results)
                }
                
            elif job_status == "failed":
                # Update job status to failed
                update_job_status(
                    groq_job,
                    status="failed",
                    error=status_data.get("error", "Unknown error"),
                    completed_at=datetime.now(timezone.utc)
                )
                
                # Update all chunks to failed
                chunks = GroqJobChunk.get_by_job_id(job_id)
                for chunk in chunks:
                    chunk.status = "failed"
                    chunk.updated_at = datetime.now(timezone.utc)
                    chunk.save()
                
                logger.info(f"Updated job {groq_job.job_id} status to failed")
                
                return {
                    "id": job_id,
                    "job_id": groq_job.job_id,
                    "status": "failed",
                    "error": status_data.get("error", "Unknown error")
                }
                
            else:
                # Job is still processing
                return {
                    "id": job_id,
                    "job_id": groq_job.job_id,
                    "status": "processing",
                    "groq_status": job_status
                }
    
    except Exception as e:
        logger.error(f"Error checking job status: {e}")
        return {"id": job_id, "status": "error", "error": str(e)}


@flow(name="Groq Batch Processing", description="Create and compile a batch of audio chunks for Groq processing")
async def batch_flow(
    source_type: Optional[SourceType] = None,
    source_id: Optional[int] = None,
    process_all: bool = False,
    max_sources_per_batch: int = 20
) -> Optional[Dict]:
    """
    Flow to create and compile a batch of audio chunks for Groq processing.
    Creates batches in memory, then compiles them (including breaking up files and uploading to S3)
    and saves them to the database.
    
    Args:
        source_type: Optional specific source type to process
        source_id: Optional specific source ID to process
        process_all: Process all available sources
        max_sources_per_batch: Maximum number of sources to process in a single batch (default: 20)
        
    Returns:
        Groq job dictionary or None if no chunks available
    """
    logger = get_run_logger()
    
    condition = True
    result_batch = None
    
    while condition:
        if source_type and source_id:
            # Handle specific source
            logger.info(f"Processing a Groq batch for {source_type} with ID {source_id}")
            
            # Get source file info
            source_info = get_source_file_info(source_type, source_id)
            if not source_info:
                logger.error(f"2 No source info found for {source_type} with ID {source_id}")
                return None
            
            # Process in memory without saving to database yet
            logger.info(f"Creating batch in memory for {source_type} with ID {source_id}")
            
            # Process the file into chunks
            file_info = {
                "source_type": source_info["source_type"],
                "source_id": source_info["source_id"],
                "local_path": source_info["local_path"],
                "name": source_info["name"]
            }
            
            # Create job in database only after processing
            job_id = f"groq_job_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_1_chunks"
            job = GroqJob(
                job_id=job_id,
                status="pending",
                created_at=datetime.now(timezone.utc)
            )
            job = job.save()
            
            # Process chunks and save to database
            processed_chunks = transcription_utils.create_transcription_chunks(file_info)
            
            # Create database records for each chunk
            for i, chunk in enumerate(processed_chunks):
                job_chunk = GroqJobChunk(
                    groq_job_id=job.id,
                    source_type=chunk["source_type"],
                    source_id=chunk["source_id"],
                    chunk_index=chunk.get("chunk_index", i),
                    name=chunk.get("name", f"chunk_{i}"),
                    s3_uri=chunk.get("s3_uri"),
                    start_time=chunk.get("start_time"),
                    end_time=chunk.get("end_time")
                )
                job_chunk.save()
            
            result_batch = {
                "id": job.id,
                "job_id": job.job_id,
                "status": job.status,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "num_chunks": len(processed_chunks)
            }
            
            logger.info(f"Created and compiled batch with job ID {job.job_id} containing {len(processed_chunks)} chunks")
            
        else:
            # Handle next available sources
            logger.info(f"Processing a Groq batch for next available entities (max {max_sources_per_batch} sources)")
            
            # Get sources ready for processing
            sources = get_sources_ready_for_processing()
            if not sources:
                logger.info("No sources available for processing")
                return None
            
            # Limit number of sources to process
            sources_to_process = sources[:max_sources_per_batch]
            num_sources = len(sources_to_process)
            logger.info(f"Processing {num_sources} sources in this batch")
            
            # Create job in database
            job_id = f"groq_job_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{num_sources}_sources"
            job = GroqJob(
                job_id=job_id,
                status="pending",
                created_at=datetime.now(timezone.utc)
            )
            job = job.save()
            
            all_processed_chunks = []
            
            # Process each source
            for source in sources_to_process:
                source_info = get_source_file_info(source["source_type"], source["source_id"])
                
                if not source_info:
                    logger.error(f"No source info found for {source['source_type']} with ID {source['source_id']}")
                    continue
                
                # Process in memory
                file_info = {
                    "source_type": source_info["source_type"],
                    "source_id": source_info["source_id"],
                    "local_path": source_info["local_path"],
                    "name": source_info["name"]
                }
                
                # Process chunks
                processed_chunks = transcription_utils.create_transcription_chunks(file_info)
                all_processed_chunks.extend(processed_chunks)
                
                logger.info(f"Processed {len(processed_chunks)} chunks for {source_info['source_type']} with ID {source_info['source_id']}")
            
            # Create database records for each chunk
            for i, chunk in enumerate(all_processed_chunks):
                job_chunk = GroqJobChunk(
                    groq_job_id=job.id,
                    source_type=chunk["source_type"],
                    source_id=chunk["source_id"],
                    chunk_index=chunk.get("chunk_index", i),
                    name=chunk.get("name", f"chunk_{i}"),
                    s3_uri=chunk.get("s3_uri"),
                    start_time=chunk.get("start_time"),
                    end_time=chunk.get("end_time")
                )
                job_chunk.save()
            
            result_batch = {
                "id": job.id,
                "job_id": job.job_id,
                "status": job.status,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "num_chunks": len(all_processed_chunks),
                "num_sources": num_sources
            }
            
            logger.info(f"Created and compiled batch with job ID {job.job_id} containing {len(all_processed_chunks)} chunks from {num_sources} sources")
        
        # Exit loop if no process_all flag or if process_all is true but no batch was created
        condition = process_all and result_batch is not None

    return result_batch

@flow(name="Submit Groq Batch", description="Submit a batch of audio chunks to Groq")
async def submit_groq_batch_flow(batch_id: Optional[int] = None, process_all: bool = False) -> Dict:
    """
    Flow to submit a batch of audio chunks to Groq
    
    Args:
        batch_id: Optional ID of an existing batch to submit
        process_all: Process all pending batches if True, otherwise just one batch
        
    Returns:
        Job status dictionary
    """
    logger = get_run_logger()
    
    results = []
    condition = True
    
    while condition:
        # Find the batch or use specified batch ID
        if batch_id:
            # Use the specified batch ID
            logger.info(f"Using specified batch ID: {batch_id}")
            groq_job = GroqJob.get_by_id(batch_id)
            if not groq_job:
                logger.error(f"Batch with ID {batch_id} not found")
                return {"error": f"Batch with ID {batch_id} not found"}
            # Turn off process_all when a specific batch is provided
            process_all = False
        else:
            # Find the earliest pending batch
            groq_job = GroqJob.get_earliest_by_status("pending")
            if not groq_job:
                logger.info("No pending batches found to submit")
                if results:
                    return {"status": "completed", "results": results, "processed_count": len(results)}
                else:
                    return {"status": "no_pending_batches"}
            batch_id = groq_job.id
            logger.info(f"Found earliest pending batch with ID: {batch_id}")
        
        # Check if batch is already submitted
        if groq_job.status != "pending":
            logger.warning(f"Batch {batch_id} has status '{groq_job.status}', not 'pending'")
            if process_all:
                # Reset batch_id and continue to the next batch
                batch_id = None
                continue
            else:
                return {"error": f"Batch {batch_id} has status '{groq_job.status}', not 'pending'"}
        
        # Get chunks for this batch
        chunks = GroqJobChunk.get_by_job_id(batch_id)
        if not chunks:
            logger.warning(f"No chunks found for batch {batch_id}")
            if process_all:
                # Reset batch_id and continue to the next batch
                batch_id = None
                continue
            else:
                return {"error": f"No chunks found for batch {batch_id}"}
        
        logger.info(f"Found {len(chunks)} chunks for batch {batch_id}")
        
        # Transform chunks to the format expected by submit_groq_job
        batch_info = {
            "id": groq_job.id,
            "job_id": groq_job.job_id,
            "status": groq_job.status,
            "chunks": [
                {
                    "id": chunk.id,
                    "source_type": chunk.source_type,
                    "source_id": chunk.source_id,
                    "chunk_index": chunk.chunk_index,
                    "start_time": chunk.start_time,
                    "end_time": chunk.end_time,
                    "s3_uri": chunk.s3_uri,
                    "name": chunk.name
                }
                for chunk in chunks
            ],
            "num_chunks": len(chunks)
        }
        
        # Submit the batch to Groq
        result = await submit_groq_job(batch_info)
        results.append(result)
        
        # If process_all is False or no result was returned, break the loop
        if not process_all or not result:
            condition = False
        else:
            # Reset batch_id to get the next pending batch
            batch_id = None
    
    # Return the last result if not processing all, otherwise return all results
    if not process_all or len(results) == 1:
        return results[0] if results else {"status": "no_results"}
    else:
        return {"status": "completed", "results": results, "processed_count": len(results)}


@flow(name="Retrieve Groq Batch", description="Retrieve a batch of audio chunks to Groq")
async def retrieve_groq_job_status_flow(batch_id: Optional[int] = None, process_all: bool = False) -> Dict:
    """
    Flow to retrieve and update the status of a Groq batch
    
    Args:
        batch_id: Optional ID of an existing batch to check
        process_all: Process all submitted batches if True, otherwise just one batch
        
    Returns:
        Job status dictionary
    """
    logger = get_run_logger()
    
    # If no batch_id is provided, find the earliest submitted batch
    if not batch_id:
        groq_job = GroqJob.get_earliest_by_status("submitted")
        if groq_job:
            batch_id = groq_job.id
            logger.info(f"Found earliest submitted batch with ID: {batch_id}")
        else:
            logger.info("No submitted batches found to retrieve")
            return {"status": "no_submitted_batches"}
    
    # Retrieve the batch status
    status = retrieve_groq_job_status(batch_id)
    
    # Update chunks if needed
    update_groq_job_chunks(batch_id)
    
    return status

@flow(name="Combine Groq Batch", description="Create a batch of audio chunks for Groq processing")
async def combine_groq_chunks_via_anthropic(
    source_type: Optional[SourceType] = None, 
    source_id: Optional[int] = None
) -> Optional[Dict]:
    logger = get_run_logger()
    
    # If source_type and source_id are provided, use them directly
    if source_type and source_id:
        logger.info(f"Processing specified entity: {source_type} with ID {source_id}")
        
        # Verify that the entity exists and has completed transcriptions
        chunks = GroqJobChunk.get_by_source(source_type, source_id)
        
        if not chunks:
            logger.error(f"No chunks found for {source_type} with ID {source_id}")
            return {"error": f"No chunks found for {source_type} with ID {source_id}"}
        
        incomplete_chunks = [chunk for chunk in chunks if not chunk.transcription]
        if incomplete_chunks:
            logger.error(f"Transcription incomplete for {source_type} with ID {source_id}. {len(incomplete_chunks)} chunks not transcribed.")
            return {"error": f"Transcription incomplete for {source_type} with ID {source_id}"}
    else:
        # Get the first entity with completed transcriptions
        completed_sources = GroqJobChunk.get_completed_sources_without_transcription()
        if not completed_sources:
            logger.error(f"No transcriptions pending")
            return {"error": f"No transcriptions pending"}
        
        source_type = completed_sources[0]["source_type"]
        source_id = completed_sources[0]["source_id"]
        logger.info(f"Processing first available entity: {source_type} with ID {source_id}")
    
    # Get chunks for this source
    chunks = GroqJobChunk.get_by_source(source_type, source_id)
    chunks.sort(key=lambda x: x.chunk_index)
    
    # Extract transcriptions
    transcriptions = [chunk.transcription for chunk in chunks if chunk.transcription]
    
    # Merge transcriptions
    if len(transcriptions) == 1:
        transcription_text = transcriptions[0]
    else:
        transcription_text = merge_multiple_transcriptions(transcriptions)
    
    # Save to database
    transcription = Transcription(
        source_type=source_type,
        source_id=source_id,
        transcription=transcription_text
    )
    transcription.save()
    
    return {
        "status": "success",
        "source_type": source_type,
        "source_id": source_id,
        "transcription_id": transcription.id,
        "chunks_merged": len(transcriptions)
    }


@flow(name="Multi-Source Transcription Pipeline", description="Main flow for multi-source transcription pipeline")
async def multi_source_transcription_pipeline(process_all: bool = True, max_sources_per_batch: int = 20):
    """
    Main flow to orchestrate the multi-source transcription pipeline.
    This is the master flow that runs the entire pipeline in sequence:
    1. Create a batch of audio chunks
    2. Submit the batch to Groq
    3. Wait for and retrieve the batch results
    4. Combine the chunks into a final transcription
    
    Args:
        process_all: Process all available sources
        max_sources_per_batch: Maximum number of sources to process in a single batch
        
    Returns:
        Summary of processed sources and their statuses
    """
    logger = get_run_logger()
    logger.info("Starting multi-source transcription pipeline")
    
    # Step 1: Create a batch of audio chunks
    logger.info("Step 1: Creating batches for processing")
    batch_result = await batch_flow(process_all=process_all, max_sources_per_batch=max_sources_per_batch)
    
    if not batch_result:
        logger.info("No sources available for processing")
        return {"status": "completed", "message": "No sources available for processing"}
    
    # Step 2: Submit the batch to Groq
    logger.info(f"Step 2: Submitting batch {batch_result['job_id']} to Groq")
    submit_result = await submit_groq_batch_flow(batch_id=batch_result['id'], process_all=process_all)
    
    if "error" in submit_result:
        logger.error(f"Error submitting batch: {submit_result['error']}")
        return {"status": "error", "message": f"Error submitting batch: {submit_result.get('error')}"}
    
    # Step 3: Wait and periodically check the status
    logger.info(f"Step 3: Waiting for Groq to process the batch")
    batch_id = batch_result['id']
    status = "submitted"
    max_attempts = 10
    attempt = 0
    
    while status == "submitted" and attempt < max_attempts:
        # Wait for 60 seconds before checking status
        logger.info(f"Waiting 60 seconds before checking batch status (attempt {attempt + 1}/{max_attempts})")
        await asyncio.sleep(60)
        
        # Check batch status
        status_result = await retrieve_groq_job_status_flow(batch_id=batch_id)
        status = status_result.get("status", "unknown")
        
        logger.info(f"Batch {batch_result['job_id']} status: {status}")
        attempt += 1
    
    if status not in ["completed", "failed", "error"]:
        logger.warning(f"Batch processing not completed after {max_attempts} attempts")
        return {
            "status": "in_progress",
            "message": f"Batch submitted but not yet completed after {max_attempts} checks",
            "batch_id": batch_id
        }
    
    if status == "failed" or status == "error":
        logger.error(f"Batch processing failed: {status_result.get('error', 'Unknown error')}")
        return {
            "status": "error",
            "message": f"Batch processing failed: {status_result.get('error', 'Unknown error')}",
            "batch_id": batch_id
        }
    
    # Step 4: Combine the chunks for sources in this batch
    logger.info(f"Step 4: Combining transcription chunks for processed sources")
    sources_processed = 0
    
    # Get all chunks for this batch and group by source
    chunks = GroqJobChunk.get_by_job_id(batch_id)
    if not chunks:
        logger.warning(f"No chunks found for batch {batch_id}")
        return {"status": "warning", "message": f"No chunks found for batch {batch_id}"}
    
    # Group chunks by source type and source ID
    chunks_by_source = {}
    for chunk in chunks:
        key = f"{chunk.source_type}_{chunk.source_id}"
        if key not in chunks_by_source:
            chunks_by_source[key] = []
        chunks_by_source[key].append(chunk)
    
    # Process each source
    combine_results = []
    for key, source_chunks in chunks_by_source.items():
        source_type, source_id_str = key.split("_")
        source_id = int(source_id_str)
        
        # Check if all chunks for this source have transcriptions
        if all(chunk.transcription for chunk in source_chunks):
            logger.info(f"Combining chunks for {source_type} with ID {source_id}")
            combine_result = await combine_groq_chunks_via_anthropic(source_type, source_id)
            combine_results.append(combine_result)
            sources_processed += 1
        else:
            logger.warning(f"Not all chunks have transcriptions for {source_type} with ID {source_id}")
    
    logger.info(f"Multi-source transcription pipeline completed. Processed {sources_processed} sources.")
    
    return {
        "status": "completed",
        "batch_id": batch_id,
        "sources_processed": sources_processed,
        "combine_results": combine_results
    }

if __name__ == "__main__":
    import asyncio
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Multi-Source Transcription Pipeline')
    parser.add_argument('--flow', choices=['batch', 'submit', 'retrieve', 'combine', 'pipeline'], 
                        help='Which flow to run (batch, submit, retrieve, combine, or full pipeline)')
    parser.add_argument('--source-type', choices=['yt_video', 'audio_file'], 
                        help='Source type for process or transcription flows')
    parser.add_argument('--source-id', type=int, help='Source ID for process or transcription flows')
    parser.add_argument('--batch-id', type=int, help='Batch ID for submit or retrieve flow')
    parser.add_argument('--process-all', action='store_true', default=False, 
                        help='If specified, process all available sources')
    parser.add_argument('--max-sources', type=int, default=20, 
                        help='Maximum number of sources to process in a single batch (default: 20)')
    
    args = parser.parse_args()
    
    if args.flow == 'batch':
        asyncio.run(batch_flow(args.source_type, args.source_id, args.process_all, args.max_sources))
    elif args.flow == 'submit':
        asyncio.run(submit_groq_batch_flow(args.batch_id, args.process_all))
    elif args.flow == 'retrieve':
        asyncio.run(retrieve_groq_job_status_flow(args.batch_id, args.process_all))
    elif args.flow == 'combine':
        # Pass the source_type and source_id to the combine flow
        asyncio.run(combine_groq_chunks_via_anthropic(args.source_type, args.source_id))
    elif args.flow == 'pipeline' or not args.flow:
        # Run the entire pipeline by default if no flow is specified
        asyncio.run(multi_source_transcription_pipeline(args.process_all, args.max_sources))
    else:
        parser.print_help()