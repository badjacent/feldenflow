from typing import List, Dict, Optional, Literal
import asyncio
import os
import json
import httpx
import psycopg2
from datetime import datetime

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

from dotenv import load_dotenv
load_dotenv()

import connection
from transcription_utils import (
    create_transcription_chunks,
    SourceType
)
from groq_utils import (
    create_groq_batch,
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
        api_key: Groq API key
        
    Returns:
        Updated job status dictionary
    """
    logger = get_run_logger()
    
    api_key = os.getenv("GROQ_API_KEY")

    
    # Get job details from database
    db_connection_string =  os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """SELECT job_id, groq_job_id, status 
               FROM groq_job 
               WHERE id = %s""",
            (job_id,)
        )
        
        row = cursor.fetchone()
        if not row:
            logger.error(f"Job with ID {job_id} not found in database")
            return {"id": job_id, "status": "error", "error": "Job not found"}
        
        db_job_id, groq_job_id, status = row
        
        # Skip if job is not in submitted status
        if status != "submitted":
            logger.info(f"Job {db_job_id} has status {status}, skipping status check")
            return {"id": job_id, "job_id": db_job_id, "status": status}
        
        # Check status with Groq API
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(
                f"https://api.groq.com/v1/audio/transcriptions/batch/{groq_job_id}",
                headers={
                    "Authorization": f"Bearer {api_key}"
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to check status of job {groq_job_id}: {response.text}")
                return {"id": job_id, "job_id": db_job_id, "status": "error", "error": response.text}
            
            status_data = response.json()
            job_status = status_data.get("status")
            
            logger.info(f"Job {db_job_id} (Groq job ID: {groq_job_id}) has status: {job_status}")
            
            # Update database based on status
            if job_status == "completed":
                # Get the results
                results = status_data.get("results", [])
                
                # Get chunk mappings
                cursor.execute(
                    """SELECT gjc.id, gjc.transcription_chunk_id, tc.file_path
                       FROM groq_job_chunk gjc
                       JOIN transcription_chunk tc ON gjc.transcription_chunk_id = tc.id
                       WHERE gjc.groq_job_id = %s""",
                    (job_id,)
                )
                
                chunk_mappings = {
                    os.path.basename(row[2]): {
                        "groq_job_chunk_id": row[0],
                        "transcription_chunk_id": row[1]
                    }
                    for row in cursor.fetchall()
                }
                
                # Update each result
                for result in results:
                    file_id = result.get("file_id")
                    transcription = result.get("text", "")
                    
                    # In a real implementation, you'd have a better way to match results to chunks
                    # Here, we're using a placeholder approach
                    for file_name, mapping in chunk_mappings.items():
                        # Match by file name or other identifiers from Groq
                        if f"placeholder_file_id_for_{file_name}" == file_id:
                            cursor.execute(
                                """UPDATE groq_job_chunk
                                   SET status = %s, updated_at = %s, transcription = %s
                                   WHERE id = %s""",
                                ("completed", datetime.now(), transcription, mapping["groq_job_chunk_id"])
                            )
                            break
                
                # Update job status
                cursor.execute(
                    """UPDATE groq_job
                       SET status = %s, completed_at = %s
                       WHERE id = %s""",
                    ("completed", datetime.now(), job_id)
                )
                
                conn.commit()
                logger.info(f"Updated job {db_job_id} status to completed")
                
                return {
                    "id": job_id,
                    "job_id": db_job_id,
                    "status": "completed",
                    "completed_at": datetime.now().isoformat(),
                    "results_count": len(results)
                }
                
            elif job_status == "failed":
                # Update job status to failed
                cursor.execute(
                    """UPDATE groq_job
                       SET status = %s, error_message = %s
                       WHERE id = %s""",
                    ("failed", status_data.get("error", "Unknown error"), job_id)
                )
                
                # Update all chunks to failed
                cursor.execute(
                    """UPDATE groq_job_chunk
                       SET status = %s, updated_at = %s
                       WHERE groq_job_id = %s""",
                    ("failed", datetime.now(), job_id)
                )
                
                conn.commit()
                logger.info(f"Updated job {db_job_id} status to failed")
                
                return {
                    "id": job_id,
                    "job_id": db_job_id,
                    "status": "failed",
                    "error": status_data.get("error", "Unknown error")
                }
                
            else:
                # Job is still processing
                return {
                    "id": job_id,
                    "job_id": db_job_id,
                    "status": "processing",
                    "groq_status": job_status
                }
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error checking job status: {e}")
        return {"id": job_id, "status": "error", "error": str(e)}
        
    finally:
        cursor.close()
        conn.close()

   

@flow(name="Create Groq Batch", description="Create a batch of audio chunks for Groq processing")
async def create_groq_batch_flow() -> Optional[Dict]:
    """
    Flow to create a batch of audio chunks for Groq processing
    
    Args:
        max_chunks: Maximum number of chunks per batch
        
    Returns:
        Groq job dictionary or None if no chunks available
    """
    logger = get_run_logger()
    logger.info(f"Creating a Groq batch")
    
    batch = create_groq_batch()
    
    if batch:
        logger.info(f"Created batch with job ID {batch['job_id']} containing {batch['num_chunks']} chunks")
    return batch

@flow(name="Submit Groq Batch", description="Submit a batch of audio chunks to Groq")
async def submit_groq_batch_flow(batch_id: Optional[int] = None) -> Dict:
    """
    Flow to submit a batch of audio chunks to Groq
    
    Args:
        batch_id: Optional ID of an existing batch to submit
        
    Returns:
        Job status dictionary
    """
    logger = get_run_logger()
    
    batch_id = 19
    if batch_id:
        # Get existing batch from database
        db_connection_string =  os.getenv('DATABASE_URL')
        conn = psycopg2.connect(db_connection_string)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                """SELECT id, job_id, status 
                   FROM groq_job 
                   WHERE id = %s""",
                (batch_id,)
            )
            
            row = cursor.fetchone()
            if not row:
                logger.error(f"Batch with ID {batch_id} not found")
                return {"error": f"Batch with ID {batch_id} not found"}
            
            batch_info = {
                "id": row[0],
                "job_id": row[1],
                "status": row[2]
            }
            
            # Get chunks for this batch
            cursor.execute(
                """SELECT id, source_type, source_id, chunk_index, 
                          start_time, end_time, s3_uri, name
                   FROM groq_job_chunk gjc 
                   WHERE gjc.groq_job_id = %s""",
                (batch_id,)
            )
            
            chunks = [
                {
                    "id": row[0],
                    "source_type": row[1],
                    "source_id": row[2],
                    "chunk_index": row[3],
                    "start_time": row[4],
                    "end_time": row[5],
                    "s3_uri": row[6],
                    "name": row[7]
                }
                for row in cursor.fetchall()
            ]
            
            logger.info(chunks)
            batch_info["chunks"] = chunks
            batch_info["num_chunks"] = len(chunks)
            
        finally:
            cursor.close()
            conn.close()
    else:
        # Create a new batch
        batch_info = await create_groq_batch_flow()
        
        if not batch_info:
            logger.info("No chunks available for processing")
            return {"status": "no_chunks"}
    
    # Submit the batch to Groq
    result = await submit_groq_job(batch_info)
    
    # logger.info(f"Submitted batch {result["id"]} to Groq with status: {result["status"]}")
    return result


@flow(name="Retrieve Groq Batch", description="Retrieve a batch of audio chunks to Groq")
async def retrieve_groq_job_status_flow(batch_id: Optional[int] = None) -> Dict:
    a = retrieve_groq_job_status(batch_id)
    update_groq_job_chunks(batch_id)
    return a

@flow(name="Combine Groq Batch", description="Create a batch of audio chunks for Groq processing")
async def combine_groq_chunks_via_anthropic() -> Optional[Dict]:
    # get next completed batch
    logger = get_run_logger()

    db_connection_string =  os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """with groq_finished as (
                    select 
                        source_type, 
                        source_id 
                    from 
                        groq_job_chunk 
                    group by 
                        source_type, 
                        source_id 
                having SUM(CASE WHEN transcription is not null THEN 1 ELSE 0 END)=SUM(1)
                )
                select 
                    groq_finished.source_type,
                    groq_finished.source_id 
                from 
                    groq_finished
                left join 
                    transcription
                on 
                    groq_finished.source_type=transcription.source_type and groq_finished.source_id=transcription.source_id
                where 
                    transcription.source_id is null
                order by 
                    transcription.source_type, 
                    transcription.source_id""",
            ()
        )
        
        row = cursor.fetchone()
        if not row:
            logger.error(f"No transcriptions pending")
            return {"error": f"No transcriptions pending"}
        
        source_type = row[0]
        source_id = row[1]
        
        # Get chunks for this batch
        cursor.execute(
            """SELECT transcription
                FROM groq_job_chunk gjc 
                WHERE gjc.source_type = %s AND gjc.source_id = %s
                ORDER BY gjc.chunk_index
                """,
            (source_type, source_id)
        )
        
        transcriptions = [
            {
                row[0]
            }
            for row in cursor.fetchall()
        ]

        transcription = ""
        if len(transcriptions) == 1:
            transcription = transcriptions[0]
        else:
            transcription = merge_multiple_transcriptions(transcriptions)

        cursor.execute(
            """INSERT INTO TRANSCRIPTION (source_type, source_id, transcription)
                VALUES(%s, %s, %s)
                """,
            (source_type, source_id, transcription)
        )
        conn.commit()
        
        
        
    finally:
        cursor.close()
        conn.close()



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
                        default='batch', help='Which flow to run')
    parser.add_argument('--source-type', choices=['yt_video', 'audio_file'], 
                        help='Source type for process or transcription flows')
    parser.add_argument('--source-id', type=int, help='Source ID for process or transcription flows')
    parser.add_argument('--batch-id', type=int, help='Batch ID for submit flow')
    
    args = parser.parse_args()
    
    if args.flow == 'batch':
        asyncio.run(create_groq_batch_flow())
    elif args.flow == 'submit':
        asyncio.run(submit_groq_batch_flow(args.batch_id))
    elif args.flow == 'retrieve':
        asyncio.run(retrieve_groq_job_status_flow(19))
    elif args.flow == 'combine':
        asyncio.run(combine_groq_chunks_via_anthropic())        