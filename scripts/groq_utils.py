import os
import subprocess
from typing import List, Dict, Optional, Tuple, Literal
from prefect import task, get_run_logger
from dotenv import load_dotenv
import math
import tempfile
import boto3
from datetime import datetime, timezone
import json
import requests
from groq import Groq
import transcription_utils
from itertools import groupby
from operator import itemgetter
from datetime import timezone


from database import (
    SourceType, 
    get_source_file_info,
    create_groq_job_with_chunks,
    get_groq_jobs_with_chunks_by_status,
    update_groq_job_after_submission,
    update_job_status,
    GroqJob,
    GroqJobChunk
)

load_dotenv()


def s3_uri_to_https_url(s3_uri: str) -> str:
    """
    Convert S3 URI (s3://bucket/key) to HTTPS URL format
    (https://bucket.s3.region.amazonaws.com/path/to/file)
    
    For example:
    s3://fmtt/DOAgszwCqUI_chunk_0.flac → https://fmtt.s3.us-east-1.amazonaws.com/flac/flac/DOAgszwCqUI_chunk_0.flac
    
    Args:
        s3_uri: S3 URI string
        
    Returns:
        HTTPS URL for the S3 object
    """
    if not s3_uri.startswith("s3://"):
        return s3_uri
    
    # Extract bucket and key from s3:// URI
    # s3://bucket/key → bucket, key
    s3_parts = s3_uri[5:].split('/', 1)
    bucket = s3_parts[0]
    key = s3_parts[1] if len(s3_parts) > 1 else ""
    
    # Extract filename from key
    filename = key.split('/')[-1]
    
    # For your specific case, add the "flac/flac/" prefix
    # and specify the region "us-east-1"
    return f"https://{bucket}.s3.us-east-1.amazonaws.com/{filename}"

def create_groq_jsonl_file(chunks: List[Dict], model: str = "whisper-large-v3", language: str = "en") -> str:
    """
    Create a JSONL file for Groq batch transcription submission
    
    Args:
        chunks: List of chunk dictionaries containing s3_uri
        model: Whisper model to use
        language: Language code for transcription
        
    Returns:
        Path to created JSONL file
    """
    logger = get_run_logger()
    
    if not chunks:
        logger.warning("No chunks provided for JSONL file creation")
        return None
    
    # Create temporary file
    fd, jsonl_path = tempfile.mkstemp(suffix=".jsonl", prefix="groq_batch_")
    os.close(fd)  # Close file descriptor but keep the file
    
    
    try:
        with open(jsonl_path, 'w') as f:
            for chunk in chunks:
                # Extract video_id and chunk_index for custom_id
                video_id = chunk.get("name", "")
                if not video_id and "path" in chunk:
                    # Try to extract from file path if video_id is not in chunk
                    base_name = os.path.basename(chunk["path"])
                    video_id = os.path.splitext(base_name)[0]
                
                # Create custom_id based on video_id and chunk_index
                custom_id = f"job-{video_id}"
                
                # Get S3 URI and convert to HTTPS URL if needed
                url = chunk.get("s3_uri", "")
                
                # Create entry object
                entry = {
                    "custom_id": custom_id,
                    "method": "POST",
                    "url": "/v1/audio/transcriptions",
                    "body": {
                        "model": model,
                        "language": language,
                        "url": s3_uri_to_https_url(url),
                        "response_format": "verbose_json",
                        "timestamp_granularities": ["segment"]
                    }

                    # {s", "body": {"model": "whisper-large-v3", "language": "en", "url": "https://example.com/audio-file.wav", "response_format": "verbose_json", "timestamp_granularities": ["segment"]}}

                }
                
                # Write as JSON line
                f.write(json.dumps(entry) + "\n")
            
        logger.info(f"Created JSONL file with {len(chunks)} entries")
        return jsonl_path
    
    except Exception as e:
        logger.error(f"Error creating JSONL file: {e}")
        # Remove the file if there was an error
        if os.path.exists(jsonl_path):
            os.unlink(jsonl_path)
        raise



def compile_groq_batch(
    source_type: Optional[SourceType] = None,
    source_id: Optional[int] = None,
    process_all: bool = False
) -> Optional[Dict]:
    """
    Compile a batch of chunks for Groq processing
    
    Args:
        source_type: Optional specific source type to process
        source_id: Optional specific source ID to process
        process_all: Whether to process all available jobs
        
    Returns:
        Groq job dictionary or None if no chunks available
    """
    logger = get_run_logger()
    
    # Get job(s) to process
    if source_type and source_id:
        logger.info(f"Compiling batch for specific entity: {source_type} with ID {source_id}")
        
        # Get the specific job and chunks
        source_info = get_source_file_info(source_type, source_id)
        if not source_info:
            logger.error(f"1 No source info found for {source_type} with ID {source_id}")
            return None
        
        # Create a job for this source
        job = create_groq_job_with_chunks([source_info])
        if not job:
            logger.error(f"Failed to create job for {source_type} with ID {source_id}")
            return None
        
        jobs = [job]
        
    else:
        # Get jobs with "pending" status
        logger.info(f"Compiling batches for jobs with 'pending' status")
        jobs = get_groq_jobs_with_chunks_by_status("pending")
        
        if not jobs:
            logger.info("No jobs with 'pending' status found")
            return None
    
    # Process all jobs
    results = []
    for job_info in jobs:
        job = job_info["job"]
        chunks = job_info["chunks"]
        
        logger.info(f"Processing job {job.id} with {len(chunks)} chunks")
        
        # Process each chunk
        for chunk in chunks:
            # Create transcription chunks
            file_info = {
                "source_type": chunk["source_type"],
                "source_id": chunk["source_id"],
                "local_path": chunk["local_path"],
                "name": chunk["name"]
            }
            
            # Process the file into chunks
            processed_chunks = transcription_utils.create_transcription_chunks(file_info)
            
            # Update each chunk in the database
            for processed_chunk in processed_chunks:
                update_groq_job_after_chunking(job.id, processed_chunk)
        
        # Add result
        results.append({
            "id": job.id,
            "job_id": job.job_id,
            "status": job.status,
            "chunks": chunks,
            "num_chunks": len(chunks)
        })
        
        # Stop after one job unless process_all is True
        if not process_all:
            break
    
    if results:
        return results[0]  # Return the first job for backward compatibility
    
    return None

   
def get_groq_jobs_with_status(status: str) -> List[Dict]:
    """
    Get jobs with a specific status, including their chunks
    
    Args:
        status: Status to filter jobs by
        
    Returns:
        List of jobs with their chunks
    """
    logger = get_run_logger()
    
    # Use the database function to get jobs with chunks
    jobs_with_chunks = get_groq_jobs_with_chunks_by_status(status)
    
    if not jobs_with_chunks:
        logger.info(f"No jobs found with status: {status}")
        return []
    
    # Transform to the expected format
    result = []
    for job_info in jobs_with_chunks:
        job = job_info["job"]
        chunks = job_info["chunks"]
        
        # Group chunks by source_type and source_id
        chunk_groups = []
        for chunk in chunks:
            chunk_groups.append(chunk)
        
        result.append(chunk_groups)
    
    logger.info(f"Found {len(result)} jobs with status {status}")
    return result
    




def update_groq_job_after_chunking(job_id: int, chunk: Dict) -> Dict:
    """
    Update the groq_job_chunk with chunk information
    
    Args:
        job_id: Database ID of the groq_job record
        chunk: Chunk information with s3_uri, etc.
        
    Returns:
        Updated job information
    """
    logger = get_run_logger()
    
    if not job_id:
        logger.error("No job_id provided for update")
        return {"status": "error", "message": "No job_id provided"}
    
    # Get the job and chunk
    job = GroqJob.get_by_id(job_id)
    if not job:
        logger.error(f"Job with ID {job_id} not found in database")
        return {"status": "error", "message": f"Job {job_id} not found"}
    
    # Find the chunk to update
    chunks = GroqJobChunk.get_by_job_id(job_id)
    for db_chunk in chunks:
        if (db_chunk.source_type == chunk["source_type"] and 
            db_chunk.source_id == chunk["source_id"] and 
            db_chunk.chunk_index == chunk.get("chunk_index", 0)):
            
            # Update chunk information
            db_chunk.s3_uri = chunk.get("s3_uri")
            db_chunk.start_time = chunk.get("start_time")
            db_chunk.end_time = chunk.get("end_time")
            db_chunk.updated_at = datetime.now(timezone.utc)
            db_chunk.save()
            
            logger.info(f"Updated chunk ID {db_chunk.id} with S3 URI: {chunk.get('s3_uri')}")
            
            return {
                "status": "success",
                "job_id": job_id,
                "chunk_id": db_chunk.id,
                "updated": True
            }
    
    logger.error(f"No matching chunk found for job ID {job_id}")
    return {"status": "error", "message": "No matching chunk found", "job_id": job_id}


def send_groq_batch(job_id: int, input_file_id: str) -> Dict:
    """
    Send a batch to Groq for processing
    
    Args:
        job_id: Database ID of the groq_job record
        input_file_id: Path to the input file (JSONL)
        
    Returns:
        Dictionary with submission information
    """
    logger = get_run_logger()

    api_key = os.getenv("GROQ_API_KEY")    
    client = Groq(api_key=api_key)
    
    try:
        # Upload the file to Groq
        response = client.files.create(file=open(input_file_id, "rb"), purpose="batch")
        
        # Create the batch
        batch_response = client.batches.create(
            completion_window="24h",
            endpoint="/v1/chat/completions",
            input_file_id=response.id,
        )

        # Prepare response
        groq_response = {
            "id": batch_response.id, 
            "status": "OK"
        }
        
        # Update the job in the database
        result = update_groq_job_after_submission(job_id, groq_response)
        
        return result
        
    except Exception as e:
        logger.error(f"Error sending batch to Groq: {e}")
        
        # Update job status to error
        job = GroqJob.get_by_id(job_id)
        if job:
            update_job_status(job, "error", error=str(e))
            
        return {
            "status": "error", 
            "message": str(e), 
            "job_id": job_id
        }

def get_groq_file_content(client, file_id: str) -> Optional[str]:
    """
    Retrieve content from a Groq file ID by writing to a temporary file and reading back.
    
    Args:
        client: Groq client instance
        file_id: ID of the file to retrieve
        
    Returns:
        File content as a string, or None if retrieval fails
    """
    logger = get_run_logger()
    logger.info(f"Retrieving Groq file content for file ID: {file_id}")
    
    # Create a temporary file with .jsonl extension
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl") as temp_file:
        temp_path = temp_file.name
    
    try:
        # Get file content from Groq
        response = client.files.content(file_id)
        
        # Write response to temporary file
        response.write_to_file(temp_path)
        logger.info(f"Wrote response to temporary file: {temp_path}")
        
        # Read the file content back into a string
        with open(temp_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        logger.info(f"Read {len(content)} bytes from file")
        return content
        
    except Exception as e:
        logger.error(f"Error retrieving Groq file content: {e}")
        return None
        
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            logger.debug(f"Removed temporary file: {temp_path}")


def retrieve_groq_job_status(job_id: int) -> Dict:
    """
    Check Groq batch status and update only the groq_job table
    without modifying chunks.
    
    Args:
        job_id: Database ID of the groq_job record
        
    Returns:
        Dictionary with job status information
    """
    logger = get_run_logger()
    
    # Get the job from database
    job = GroqJob.get_by_id(job_id)
    if not job:
        logger.error(f"Job with ID {job_id} not found in database")
        return {"status": "error", "message": f"Job {job_id} not found"}
    
    if not job.groq_job_id:
        logger.error(f"Job {job_id} ({job.job_id}) has no Groq job ID")
        return {"status": "error", "message": "No Groq job ID found"}
    
    # Initialize Groq client
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        logger.error("GROQ_API_KEY environment variable not set")
        return {"status": "error", "message": "GROQ_API_KEY environment variable not set"}
    
    client = Groq(api_key=api_key)
    
    # Retrieve batch results from Groq
    logger.info(f"Checking status for Groq job ID: {job.groq_job_id}")
    try:
        response = client.batches.retrieve(job.groq_job_id).to_dict()
    except Exception as e:
        logger.error(f"Error retrieving batch from Groq: {e}")
        
        # Update the error field
        update_job_status(job, "error", error=str(e))
        
        return {"status": "error", "message": f"Error retrieving from Groq: {str(e)}"}
    
    # Process batch status
    batch_status = response["status"]
    
    # Get output file content if available
    output_file_id = response.get("output_file_id")
    output_content = None
    
    if output_file_id:
        try:
            output_content = get_groq_file_content(client, output_file_id)
            logger.info(f"Retrieved output file content for job ID: {job_id}")
        except Exception as e:
            logger.error(f"Error retrieving output file: {e}")
            # Add error to existing error field
            current_error = job.error or ""
            error_msg = f"Error retrieving output file: {str(e)}"
            if current_error:
                error_msg = f"{current_error}\n{error_msg}"
            update_job_status(job, job.status, error=error_msg)
    
    # Get error file content if available
    error_file_id = response.get("error_file_id")
    error_content = None
    
    if error_file_id:
        try:
            error_content = get_groq_file_content(client, error_file_id)
            logger.info(f"Retrieved error file content for job ID: {job_id}")
        except Exception as e:
            logger.error(f"Error retrieving error file: {e}")
            # Add error to existing error field
            current_error = job.error or ""
            error_msg = f"Error retrieving error file: {str(e)}"
            if current_error:
                error_msg = f"{current_error}\n{error_msg}"
            update_job_status(job, job.status, error=error_msg)
    
    # Determine if job is completed or failed
    is_complete = batch_status in ["completed", "failed"]
    completed_at = datetime.now(timezone.utc) if is_complete and batch_status != job.status else None
    if not (is_complete):
        batch_status = "submitted"
    
    # Update job status
    update_job_status(
        job, 
        status=batch_status, 
        completed_at=completed_at,
        output=output_content,
        error=error_content
    )
    
    return {
        "status": batch_status,
        "job_id": job_id,
        "groq_job_id": job.groq_job_id,
        "completed_at": completed_at.isoformat() if completed_at else None,
        "has_output": output_content is not None,
        "has_error": error_content is not None
    }

def update_groq_job_chunks(job_id: int) -> Dict:
    """
    Update groq_job_chunk entries based on the groq_job status and output content.
    Parses the JSONL output field to correlate results with chunks.
    
    Args:
        job_id: Database ID of the groq_job record
        
    Returns:
        Dictionary with update results
    """
    logger = get_run_logger()
    
    # Get the groq_job record
    job = GroqJob.get_by_id(job_id)
    if not job:
        logger.error(f"Job with ID {job_id} not found in database")
        return {"status": "error", "message": f"Job {job_id} not found"}
    
    logger.info(f"Updating chunks for job {job.id} ({job.job_id}) with status: {job.status}")
    
    # Get all chunks for this job
    chunks = GroqJobChunk.get_by_job_id(job_id)
    if not chunks:
        logger.warning(f"No chunks found for job {job_id}")
        return {"status": "warning", "message": "No chunks found", "job_id": job_id}
    
    # Create a mapping from custom_id to chunk
    chunk_mapping = {}
    for chunk in chunks:
        # Generate custom_id based on our format
        custom_id = f"job-{chunk.name}"
        chunk_mapping[custom_id] = chunk
    
    logger.info(f"Found {len(chunk_mapping)} chunks to process")
    
    # Parse JSONL output if available
    results_processed = 0
    errors_found = 0
    
    # Default chunk status based on job status
    default_status = job.status
    if job.status == "completed":
        default_status = "results not found"  # Default for chunks without specific results
    
    update_time = job.completed_at or datetime.now(timezone.utc)
    
    # Process output content if available
    if job.output:
        try:
            # Parse each line of JSONL output
            for line in job.output.splitlines():
                if line.strip():
                    try:
                        result_data = json.loads(line)
                        custom_id = result_data.get("custom_id")
                        
                        # Find the corresponding chunk
                        if custom_id in chunk_mapping:
                            chunk = chunk_mapping[custom_id]
                            
                            # Extract transcription text from result
                            # The structure depends on the Groq API response format
                            transcription = ""
                            
                            # Check for different result structures
                            if ("response" in result_data and 
                                "body" in result_data["response"] and 
                                "text" in result_data["response"]["body"]):
                                # Standard format
                                transcription = result_data["response"]["body"]["text"]
                            
                            # Update the chunk
                            if transcription:
                                chunk.transcription = transcription
                                chunk.updated_at = update_time
                                chunk.save()
                                
                                results_processed += 1
                                logger.debug(f"Updated chunk {custom_id} with transcription")
                        else:
                            logger.warning(f"No matching chunk found for custom_id: {custom_id}")
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing output line: {e}")
                        errors_found += 1
                        
        except Exception as e:
            logger.error(f"Error processing output content: {e}")
            return {"status": "error", "message": f"Error processing output: {str(e)}"}
    
    # Process error content if available
    if job.error:
        try:
            # Try to parse error content as JSONL
            for line in job.error.splitlines():
                if line.strip():
                    try:
                        error_data = json.loads(line)
                        custom_id = error_data.get("custom_id")
                        
                        # Find the corresponding chunk
                        if custom_id in chunk_mapping:
                            chunk = chunk_mapping[custom_id]
                            
                            # Update the chunk status
                            chunk.status = "failed"
                            chunk.updated_at = update_time
                            chunk.save()
                            
                            errors_found += 1
                            logger.debug(f"Updated chunk {custom_id} with error status")
                        else:
                            logger.warning(f"No matching chunk found for custom_id: {custom_id}")
                            
                    except json.JSONDecodeError:
                        # Not valid JSON, might be a general error message
                        # Just log and continue
                        pass
                        
        except Exception as e:
            logger.error(f"Error processing error content: {e}")
            # Continue processing
    
    # Update any remaining chunks that weren't in the output
    if job.status in ["completed", "failed"]:
        for chunk in chunks:
            if not chunk.transcription and chunk.status not in ["completed", "failed"]:
                chunk.status = default_status
                chunk.updated_at = update_time
                chunk.save()
    
    return {
        "status": "success",
        "job_id": job_id,
        "job_status": job.status,
        "results_processed": results_processed,
        "errors_found": errors_found,
    }


