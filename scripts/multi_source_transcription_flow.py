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
    YTChannel, 
    YTVideo,
    YTPrivateChannelVideo,
    AudioFile,
    Transcription,
    GroqJob,
    GroqJobChunk,
    GroqJobStatus,
    get_sources_ready_for_processing,
    get_file_path_for_source,
    create_job_chunk,
    update_job_status,
    merge_transcriptions,
    get_source_file_info
)

from transcription_utils import (
    create_transcription_chunks,
)
from groq_utils import (
    compile_groq_batch,
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
                # Job is still in submitted state
                return {
                    "id": job_id,
                    "job_id": groq_job.job_id,
                    "status": "submitted",
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
    
    results = []
    condition = True
    
    while condition:
        current_batch_id = batch_id
        
        # If no batch_id is provided, find the earliest submitted batch
        if not current_batch_id:
            groq_job = GroqJob.get_earliest_by_status("submitted")
            if groq_job:
                current_batch_id = groq_job.id
                logger.info(f"Found earliest submitted batch with ID: {current_batch_id}")
            else:
                logger.info("No submitted batches found to retrieve")
                if results:
                    return {"status": "completed", "results": results, "processed_count": len(results)}
                else:
                    return {"status": "no_submitted_batches"}
        else:
            # If a specific batch ID is provided, turn off process_all
            process_all = False
        
        # Retrieve the batch status
        status = retrieve_groq_job_status(current_batch_id)
        
        # Update chunks if needed
        update_groq_job_chunks(current_batch_id)
        
        # Add to results
        results.append(status)
        
        # If process_all is False or no result was returned, break the loop
        if not process_all or not status:
            condition = False
        else:
            # Reset batch_id to get the next submitted batch
            batch_id = None
    
    # Return the last result if not processing all, otherwise return all results
    if not process_all or len(results) == 1:
        return results[0] if results else {"status": "no_results"}
    else:
        return {"status": "completed", "results": results, "processed_count": len(results)}

@flow(name="Combine Groq Batch", description="Create a batch of audio chunks for Groq processing")
async def combine_groq_chunks_via_anthropic(
    source_type: Optional[SourceType] = None, 
    source_id: Optional[int] = None,
    process_all: bool = False
) -> Optional[Dict]:
    logger = get_run_logger()
    
    results = []
    condition = True
    
    while condition:
        current_source_type = source_type
        current_source_id = source_id
        
        # If source_type and source_id are provided, use them directly
        if current_source_type and current_source_id:
            logger.info(f"Processing specified entity: {current_source_type} with ID {current_source_id}")
            
            # Verify that the entity exists and has completed transcriptions
            chunks = GroqJobChunk.get_by_source(current_source_type, current_source_id)
            
            if not chunks:
                logger.error(f"No chunks found for {current_source_type} with ID {current_source_id}")
                if results:
                    return {"status": "completed", "results": results, "processed_count": len(results)}
                else:
                    return {"error": f"No chunks found for {current_source_type} with ID {current_source_id}"}
            
            incomplete_chunks = [chunk for chunk in chunks if not chunk.transcription]
            if incomplete_chunks:
                logger.error(f"Transcription incomplete for {current_source_type} with ID {current_source_id}. {len(incomplete_chunks)} chunks not transcribed.")
                if results:
                    return {"status": "completed", "results": results, "processed_count": len(results)}
                else:
                    return {"error": f"Transcription incomplete for {current_source_type} with ID {current_source_id}"}
                
            # Once we've processed a specific source, turn off process_all
            process_all = False
        else:
            # Get the completed sources that don't have a merged transcription yet
            completed_sources = GroqJobChunk.get_completed_sources_without_transcription()
            if not completed_sources:
                logger.info("No transcriptions pending")
                if results:
                    return {"status": "completed", "results": results, "processed_count": len(results)}
                else:
                    return {"error": "No transcriptions pending"}
            
            current_source_type = completed_sources[0]["source_type"]
            current_source_id = completed_sources[0]["source_id"]
            logger.info(f"Processing entity: {current_source_type} with ID {current_source_id}")
        
        # Get chunks for this source
        chunks = GroqJobChunk.get_by_source(current_source_type, current_source_id)
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
            source_type=current_source_type,
            source_id=current_source_id,
            transcription=transcription_text
        )
        transcription.save()
        
        result = {
            "status": "success",
            "source_type": current_source_type,
            "source_id": current_source_id,
            "transcription_id": transcription.id,
            "chunks_merged": len(transcriptions)
        }
        results.append(result)
        
        # If process_all is False or we just processed a specific source, break the loop
        if not process_all:
            condition = False
        else:
            # Reset source_type and source_id to get the next source
            source_type = None
            source_id = None
    
    # Return the last result if not processing all, otherwise return all results
    if len(results) == 1:
        return results[0]
    else:
        return {"status": "completed", "results": results, "processed_count": len(results)}


@flow(name="Multi-Source Transcription Pipeline", description="Main flow for multi-source transcription pipeline")
async def multi_source_transcription_pipeline():
    """
    Main flow to orchestrate the multi-source transcription pipeline
    """
    logger = get_run_logger()
    logger.info("Starting multi-source transcription pipeline")
    
    logger.info("Multi-source transcription pipeline completed")

if __name__ == "__main__":
    import asyncio
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Multi-Source Transcription Pipeline')
    parser.add_argument('--flow', choices=['batch', 'submit', 'retrieve', 'combine'], 
                        help='Which flow to run')
    parser.add_argument('--source-type', choices=['yt_video', 'audio_file'], 
                        help='Source type for process or transcription flows')
    parser.add_argument('--source-id', type=int, help='Source ID for process or transcription flows')
    parser.add_argument('--batch-id', type=int, help='Batch ID for submit or retrieve flow')
    parser.add_argument('--process-all', type=bool, default=False, help='If true then process all')
    parser.add_argument('--max-sources', type=int, default=20, 
                        help='Maximum number of sources to process in a single batch (default: 20)')

    
    args = parser.parse_args()
    process_all = args.process_all
    
    if args.flow == 'batch':
        asyncio.run(batch_flow(args.source_type, args.source_id, process_all, args.max_sources))

    elif args.flow == 'submit':
        asyncio.run(submit_groq_batch_flow(args.batch_id, process_all))
    elif args.flow == 'retrieve':
        asyncio.run(retrieve_groq_job_status_flow(args.batch_id, process_all))
    elif args.flow == 'combine':
        # Pass the source_type, source_id, and process_all to the combine flow
        asyncio.run(combine_groq_chunks_via_anthropic(args.source_type, args.source_id, process_all))        