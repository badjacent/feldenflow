import os
import subprocess
from typing import List, Dict, Optional, Tuple, Literal
import psycopg2
from prefect import task, get_run_logger
from dotenv import load_dotenv
import math
import tempfile
import boto3
from datetime import datetime
import json
import requests
from groq import Groq
import transcription_utils


SourceType = Literal["yt_video", "audio_file"]

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



def create_groq_batch() -> Optional[Dict]:
    """
    Create a batch of chunks for Groq processing
    
    Args:
        max_chunks: Maximum number of chunks per batch
        
    Returns:
        Groq job dictionary or None if no chunks available
    """
    logger = get_run_logger()

    chunks = transcription_utils.get_next_groq_batch('yt_video')
    if not chunks:
        logger.info("No chunks available for transcription")
        return None

    # Get chunks that need transcription
    db_connection_string =  os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:       
        # Create a new Groq job
        job_id = f"groq_job_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(chunks)}_chunks"
        
        cursor.execute(
            """INSERT INTO groq_job
               (job_id, status, created_at)
               VALUES (%s, %s, %s)
               RETURNING id""",
            (job_id, "pending", datetime.now())
        )
        
        groq_job_id = cursor.fetchone()[0]
        
        # Associate chunks with the job
        for chunk in chunks:
            logger.info(chunk)
            cursor.execute(
                """INSERT INTO groq_job_chunk
                   (groq_job_id, source_type, source_id, chunk_index, name, status, s3_uri, start_time, end_time)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (groq_job_id, chunk["source_type"], chunk["source_id"], chunk["chunk_index"], chunk["name"], "pending", chunk["s3_uri"], chunk["start_time"], chunk["end_time"])
            )


        conn.commit()
        
        job = {
            "id": groq_job_id,
            "job_id": job_id,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "chunks": chunks,
            "num_chunks": len(chunks)
        }
        
        logger.info(f"Created Groq job {job_id} with {len(chunks)} chunks")
        return job
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating Groq batch: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()

def update_groq_job_after_submission(job_id: int, groq_response: Dict) -> Dict:
    """
    Update the groq_job record after submitting to Groq API
    
    Args:
        job_id: Database ID of the groq_job record
        groq_response: Response from Groq API submission
        
    Returns:
        Updated job information
    """
    logger = get_run_logger()
    
    if not job_id:
        logger.error("No job_id provided for update")
        return {"status": "error", "message": "No job_id provided"}
    
    # Extract Groq job ID from response
    groq_job_id = groq_response["id"]
    if not groq_job_id:
        logger.error("No Groq job ID found in response")
        return {"status": "error", "message": "No Groq job ID in response", "job_id": job_id}
    
    # Current timestamp for submission
    submitted_at = datetime.now()
    
    # Connect to database
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cursor = conn.cursor()
    
    try:
        # Update the groq_job record
        cursor.execute(
            """UPDATE groq_job
               SET status = %s,
                   groq_job_id = %s,
                   submitted_at = %s                   
               WHERE id = %s
               RETURNING job_id, status""",
            (
                "submitted",
                groq_job_id,
                submitted_at,
                job_id
            )
        )
        
        result = cursor.fetchone()
        
        if not result:
            conn.rollback()
            logger.error(f"Job with ID {job_id} not found in database")
            return {"status": "error", "message": f"Job {job_id} not found", "job_id": job_id}
        
        # Update status for all chunks in this job
        cursor.execute(
            """UPDATE groq_job_chunk
               SET status = %s, updated_at = %s
               WHERE groq_job_id = %s""",
            ("submitted", submitted_at, job_id)
        )
        
        # Commit the transaction
        conn.commit()
        
        logger.info(f"Updated job {result[0]} status to 'submitted' with Groq job ID: {groq_job_id}")
        
        return {
            "status": "success",
            "job_id": job_id,
            "groq_job_id": groq_job_id,
            "submitted_at": submitted_at.isoformat(),
            "job_status": "submitted"
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating job status: {e}")
        return {"status": "error", "message": str(e), "job_id": job_id}
        
    finally:
        cursor.close()
        conn.close()


def send_groq_batch(job_id, input_file_id):

    logger = get_run_logger()

    api_key = os.getenv("GROQ_API_KEY")    
    client = Groq(api_key=api_key)
    response = client.files.create(file=open(input_file_id, "rb"), purpose="batch")
    
    response = client.batches.create(
        completion_window="24h",
        endpoint="/v1/chat/completions",
        input_file_id=response.id,
    )

    r = {
        "id": response.id, "status": "OK"
    }
    update_groq_job_after_submission(job_id, r)

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
    
    # Connect to database
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cursor = conn.cursor()
    
    try:
        # Get the Groq job ID for this job
        cursor.execute(
            """SELECT groq_job_id, job_id, status FROM groq_job WHERE id = %s""",
            (job_id,)
        )
        
        result = cursor.fetchone()
        if not result:
            logger.error(f"Job with ID {job_id} not found in database")
            return {"status": "error", "message": f"Job {job_id} not found"}
        
        groq_job_id, local_job_id, current_status = result
        
        if not groq_job_id:
            logger.error(f"Job {job_id} ({local_job_id}) has no Groq job ID")
            return {"status": "error", "message": "No Groq job ID found"}
        
        # Initialize Groq client
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            logger.error("GROQ_API_KEY environment variable not set")
            return {"status": "error", "message": "GROQ_API_KEY environment variable not set"}
        
        client = Groq(api_key=api_key)
        
        # Retrieve batch results from Groq
        logger.info(f"Checking status for Groq job ID: {groq_job_id}")
        try:
            response = client.batches.retrieve(groq_job_id).to_dict()
        except Exception as e:
            logger.error(f"Error retrieving batch from Groq: {e}")
            
            # Update the error field in groq_job table
            cursor.execute(
                """UPDATE groq_job
                   SET error = %s
                   WHERE id = %s""",
                (str(e), job_id)
            )
            conn.commit()
            
            return {"status": "error", "message": f"Error retrieving from Groq: {str(e)}"}
        
        # Process batch status
        batch_status = response["status"]
        
        # Get output file content if available
        output_file_id = response["output_file_id"]
        output_content = None
        
        if output_file_id:
            try:
                output_content = get_groq_file_content(client, output_file_id)
                logger.info(f"Retrieved output file content for job ID: {job_id}")
            except Exception as e:
                logger.error(f"Error retrieving output file: {e}")
                # Store error but continue
                cursor.execute(
                    """UPDATE groq_job
                       SET error = CASE 
                                WHEN error IS NULL THEN %s 
                                ELSE error || E'\n' || %s 
                            END
                       WHERE id = %s""",
                    (f"Error retrieving output file: {str(e)}", f"Error retrieving output file: {str(e)}", job_id)
                )
        
        # Get error file content if available
        error_file_id = response["error_file_id"]
        error_content = None
        
        if error_file_id:
            try:
                error_content = get_groq_file_content(client, error_file_id)
                logger.info(f"Retrieved error file content for job ID: {job_id}")
            except Exception as e:
                logger.error(f"Error retrieving error file: {e}")
                # Store error but continue
                cursor.execute(
                    """UPDATE groq_job
                       SET error = CASE 
                                WHEN error IS NULL THEN %s 
                                ELSE error || E'\n' || %s 
                            END
                       WHERE id = %s""",
                    (f"Error retrieving error file: {str(e)}", f"Error retrieving error file: {str(e)}", job_id)
                )
        
        # Determine if job is completed or failed
        is_complete = batch_status in ["completed", "failed"]
        completed_at = datetime.now() if is_complete and batch_status != current_status else None
        
        # Update groq_job with all available information
        # Build the update query dynamically based on what data we have
        update_parts = ["status = %s"]
        update_values = [batch_status]
        
        if completed_at:
            update_parts.append("completed_at = %s")
            update_values.append(completed_at)
        
        if output_content is not None:
            update_parts.append("output = %s")
            update_values.append(output_content)
        
        if error_content is not None:
            update_parts.append("error = %s")
            update_values.append(error_content)
        
        # Add job_id as the last parameter
        update_values.append(job_id)

        # Execute the update query
        update_query = f"""UPDATE groq_job SET {", ".join(update_parts)} WHERE id = %s"""
        cursor.execute(update_query, update_values)
        conn.commit()
        
        return {
            "status": batch_status,
            "job_id": job_id,
            "groq_job_id": groq_job_id,
            "completed_at": completed_at.isoformat() if completed_at else None,
            "has_output": output_content is not None,
            "has_error": error_content is not None
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating Groq job status: {e}")
        return {"status": "error", "message": str(e)}
        
    finally:
        cursor.close()
        conn.close()

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
    
    # Connect to database
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cursor = conn.cursor()
    
    try:
        # Get the groq_job record
        cursor.execute(
            """SELECT id, job_id, status, output, error, completed_at 
               FROM groq_job WHERE id = %s""",
            (job_id,)
        )
        
        result = cursor.fetchone()
        if not result:
            logger.error(f"Job with ID {job_id} not found in database")
            return {"status": "error", "message": f"Job {job_id} not found"}
        
        db_id, job_id_str, status, output, error, completed_at = result
        
        logger.info(f"Updating chunks for job {db_id} ({job_id_str}) with status: {status}")
        
        # Get all chunks for this job
        cursor.execute(
            """SELECT gjc.id, gjc.source_type, gjc.source_id, gjc.chunk_index, gjc.name
               FROM groq_job_chunk gjc
               WHERE gjc.groq_job_id = %s""",
            (db_id,)
        )
        
        chunks = cursor.fetchall()
        if not chunks:
            logger.warning(f"No chunks found for job {db_id}")
            return {"status": "warning", "message": "No chunks found", "job_id": db_id}
        
        # Create a mapping from custom_id to chunk ID
        chunk_mapping = {}
        for gjc_id, source_type, source_id, chunk_index, name in chunks:
            # Extract video_id from file_path
            video_id = name
            
            # Generate custom_id based on our format
            custom_id = f"job-{video_id}"
            
            chunk_mapping[custom_id] = {
                "gjc_id": gjc_id,
                "source_type": source_type,
                "source_id": source_id,
                "chunk_index": chunk_index
            }


        logger.info(chunk_mapping)
        
        # Parse JSONL output if available
        results_processed = 0
        errors_found = 0
        
        # Default chunk status based on job status
        default_status = status
        if status == "completed":
            default_status = "results not found"  # Default for chunks without specific results
        
        update_time = completed_at or datetime.now()
        
        # Process output content if available
        if output:
            try:
                # Parse each line of JSONL output
                for line in output.splitlines():
                    if line.strip():
                        try:
                            result_data = json.loads(line)
                            custom_id = result_data.get("custom_id")

                            logger.info(custom_id)
                            
                            # Find the corresponding chunk
                            if custom_id in chunk_mapping:
                                chunk_info = chunk_mapping[custom_id]
                                gjc_id = chunk_info["gjc_id"]
                                
                                # Extract transcription text from result
                                # The structure depends on the Groq API response format
                                transcription = ""
                                
                                # Check for different result structures

                                logger.info(result_data["response"]["body"].keys())                            

                                if "response" in result_data and "body" in result_data["response"]  and "text" in result_data["response"]["body"]:
                                    # Standard format
                                    transcription = result_data["response"]["body"]["text"]
                                # elif "text" in result_data:
                                #     # Alternative format
                                #     transcription = result_data["text"]
                                    
                                # Determine status
                                chunk_status = "completed" if transcription else "transcription not found"
                                
                                # Update the groq_job_chunk record
                                cursor.execute(
                                    """UPDATE groq_job_chunk
                                       SET status = %s,
                                           updated_at = %s,
                                           transcription = %s
                                       WHERE id = %s""",
                                    (
                                        chunk_status,
                                        update_time,
                                        transcription,
                                        gjc_id
                                    )
                                )
                                
                                results_processed += 1
                                logger.debug(f"Updated chunk {custom_id} with status {chunk_status}")
                            else:
                                logger.warning(f"No matching chunk found for custom_id: {custom_id}")
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing output line: {e}")
                            errors_found += 1
                            
            except Exception as e:
                logger.error(f"Error processing output content: {e}")
                return {"status": "error", "message": f"Error processing output: {str(e)}"}
        
        # Process error content if available
        if error:
            try:
                # Try to parse error content as JSONL
                for line in error.splitlines():
                    if line.strip():
                        try:
                            error_data = json.loads(line)
                            custom_id = error_data.get("custom_id")
                            
                            # Find the corresponding chunk
                            if custom_id in chunk_mapping:
                                chunk_info = chunk_mapping[custom_id]
                                gjc_id = chunk_info["gjc_id"]
                                
                                # Update the groq_job_chunk record
                                cursor.execute(
                                    """UPDATE groq_job_chunk
                                       SET status = %s,
                                           updated_at = %s
                                       WHERE id = %s""",
                                    (
                                        "failed",
                                        update_time,
                                        gjc_id
                                    )
                                )
                                
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
        if status in ["completed", "failed"]:
            # Set default status for chunks without specific results
            cursor.execute(
                """UPDATE groq_job_chunk
                   SET status = %s,
                       updated_at = %s
                   WHERE groq_job_id = %s
                   AND status NOT IN ('completed', 'failed')""",
                (default_status, update_time, db_id)
            )
            updated_remaining = cursor.rowcount
            logger.info(f"Set default status {default_status} for {updated_remaining} remaining chunks")
        
        # Commit the transaction
        conn.commit()
        
        # Get count of chunks by status
        cursor.execute(
            """SELECT status, COUNT(*) 
               FROM groq_job_chunk 
               WHERE groq_job_id = %s 
               GROUP BY status""",
            (db_id,)
        )
        
        status_counts = {status: count for status, count in cursor.fetchall()}
        
        return {
            "status": "success",
            "job_id": db_id,
            "job_status": status,
            "results_processed": results_processed,
            "errors_found": errors_found,
            "chunk_status_counts": status_counts
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating chunks: {e}")
        return {"status": "error", "message": str(e)}
        
    finally:
        cursor.close()
        conn.close()        


def check_pending_groq_jobs(limit: int = 10) -> Dict:
    """
    Check status of all pending or submitted Groq jobs
    
    Args:
        limit: Maximum number of jobs to check
        
    Returns:
        Dictionary with check results
    """
    logger = get_run_logger()
    
    # Connect to database
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cursor = conn.cursor()
    
    try:
        # Get jobs that are in pending or submitted status
        cursor.execute(
            """SELECT id FROM groq_job 
               WHERE status IN ('pending', 'submitted', 'processing')
               ORDER BY created_at ASC
               LIMIT %s""",
            (limit,)
        )
        
        jobs = [row[0] for row in cursor.fetchall()]
        
        if not jobs:
            logger.info("No pending Groq jobs to check")
            return {"status": "no_jobs", "jobs_checked": 0}
        
        logger.info(f"Checking status for {len(jobs)} Groq jobs")
        
        # Check each job
        results = []
        for job_id in jobs:
            result = update_groq_job_status(job_id)
            results.append(result)
        
        # Summarize results
        completed = sum(1 for r in results if r["status"] == "completed")
        failed = sum(1 for r in results if r["status"] == "failed")
        processing = sum(1 for r in results if r["status"] in ["processing", "pending", "submitted"])
        errors = sum(1 for r in results if r["status"] == "error")
        
        logger.info(f"Checked {len(results)} jobs: {completed} completed, {failed} failed, {processing} still processing, {errors} errors")
        
        return {
            "status": "success",
            "jobs_checked": len(results),
            "completed": completed,
            "failed": failed,
            "processing": processing,
            "errors": errors,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error checking Groq jobs: {e}")
        return {"status": "error", "message": str(e)}
        
    finally:
        cursor.close()
        conn.close()


