import os
import subprocess
from typing import List, Dict, Optional, Tuple, Literal
from prefect import task, get_run_logger
from dotenv import load_dotenv
import math
import tempfile

load_dotenv()


# Source type literals
SourceType = Literal["yt_video", "audio_file"]



def get_audio_duration(input_file: str) -> float:
    """
    Get duration of audio file in seconds
    
    Args:
        input_file: Path to audio file
        
    Returns:
        Duration in seconds
    """
    logger = get_run_logger()
    
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", input_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        duration = float(result.stdout.strip())
        logger.info(f"Audio duration for {os.path.basename(input_file)}: {duration} seconds")
        return duration
    except Exception as e:
        logger.error(f"Error getting audio duration for {input_file}: {e}")
        raise

def get_s3_client():
    """Initialize and return an S3 client"""
    logger = get_run_logger()
    
    try:
        import boto3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        return s3_client
    except ImportError:
        logger.error("boto3 library not found. Install with: pip install boto3")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        raise

def convert_to_flac(input_file: str, output_file: str, start_time: float = None, end_time: float = None) -> str:
    """
    Convert audio file to FLAC format
    
    Args:
        input_file: Path to input audio file
        output_file: Path for output FLAC file
        start_time: Optional start time for clip
        end_time: Optional end time for clip
        
    Returns:
        Path to output FLAC file
    """
    logger = get_run_logger()
    
    cmd = ["ffmpeg", "-y"]  # Overwrite output files
    
    # Add time segment if specified
    if start_time is not None:
        cmd.extend(["-ss", str(start_time)])
    
    cmd.extend(["-i", input_file])
    
    if end_time is not None:
        cmd.extend(["-to", str(end_time)])
    
    # Add conversion parameters
    cmd.extend([
        "-ar", "16000",  # Downsample to 16kHz
        "-ac", "1",      # Convert to mono
        "-c:a", "flac",  # Use FLAC codec
        output_file
    ])
    
    try:
        subprocess.run(
            cmd, 
            check=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        )
        logger.info(f"Successfully converted to FLAC: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Error converting to FLAC: {e}")
        raise

def upload_to_s3(file_path: str, s3_bucket: str, s3_key: str) -> str:
    """
    Upload file to S3 bucket
    
    Args:
        file_path: Path to local file
        s3_bucket: S3 bucket name
        s3_key: S3 object key
        
    Returns:
        S3 URI for uploaded file
    """
    logger = get_run_logger()
    
    try:
        s3_client = get_s3_client()
        s3_client.upload_file(file_path, s3_bucket, s3_key)
        s3_uri = f"s3://{s3_bucket}/{s3_key}"
        logger.info(f"Uploaded {file_path} to {s3_uri}")
        return s3_uri
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise


def process_single_chunk(
    input_file: str, 
    source_type: str, 
    source_id: int, 
    temp_dir: str, 
    s3_bucket: str, 
    s3_folder: str
) -> Dict:
    """
    Process a file as a single chunk
    
    Args:
        input_file: Path to input audio file
        source_type: Type of source
        source_id: ID of source
        temp_dir: Temporary directory for file processing
        s3_bucket: S3 bucket name
        s3_folder: S3 folder prefix
        
    Returns:
        Chunk dictionary
    """
    logger = get_run_logger()
    
    # Get file duration
    duration = get_audio_duration(input_file)
    
    # Convert to FLAC
    base_name = os.path.splitext(os.path.basename(input_file))[0];
    output_flac = os.path.join(temp_dir, f"{os.path.splitext(os.path.basename(input_file))[0]}.flac")
    convert_to_flac(input_file, output_flac)
    
    # Upload to S3
    s3_key = f"{s3_folder}{os.path.basename(output_flac)}"
    s3_uri = upload_to_s3(output_flac, s3_bucket, s3_key)


    
    # Create chunk record
    chunk_info = {
        "chunk_index": 0,
        "start_time": 0,
        "end_time": duration,
        "path": output_flac,
        "file_size_bytes": os.path.getsize(output_flac),
        "s3_uri": s3_uri,
        "source_type": source_type,
        "source_id": source_id,
        "name": base_name
    }
   
    # Insert into database
    return chunk_info

def process_multiple_chunks(
    input_file: str, 
    source_type: str, 
    source_id: int, 
    temp_dir: str, 
    s3_bucket: str, 
    s3_folder: str,
    num_chunks: int,
    duration: float,
    overlap_seconds: int,
    file_info: Dict
) -> List[Dict]:
    """
    Process a file as multiple chunks
    
    Args:
        input_file: Path to input audio file
        source_type: Type of source
        source_id: ID of source
        temp_dir: Temporary directory for file processing
        s3_bucket: S3 bucket name
        s3_folder: S3 folder prefix
        num_chunks: Number of chunks to create
        duration: Duration of audio file in seconds
        overlap_seconds: Overlap between chunks in seconds
        file_info: File information dictionary
        
    Returns:
        List of chunk dictionaries
    """
    logger = get_run_logger()
    logger.info(f"Splitting file into {num_chunks} chunks with {overlap_seconds}s overlap")
    
    # Calculate chunk duration (without overlap)
    base_chunk_duration = duration / num_chunks
    
    # Create base name for chunks
    if source_type == "yt_video":
        base_name = file_info.get('video_id', f"video_{source_id}")
    else:
        base_name = os.path.splitext(file_info.get('original_filename', f"audio_{source_id}"))[0]
    
    chunks = []
    
    for i in range(num_chunks):
        # Calculate start and end times with overlap
        if i == 0:
            start_time = 0
        else:
            start_time = (i * base_chunk_duration) - overlap_seconds
            start_time = max(0, start_time)  # Ensure we don't go below 0
        
        if i == num_chunks - 1:
            end_time = duration
        else:
            end_time = ((i + 1) * base_chunk_duration) + overlap_seconds
            end_time = min(duration, end_time)  # Ensure we don't exceed duration
        
        # Create FLAC output file name
        name = f"{base_name}_chunk_{i}"
        output_flac = os.path.join(temp_dir, f"{name}.flac")
        
        # Convert chunk to FLAC
        logger.info(f"Creating chunk {i+1}/{num_chunks}: {start_time:.2f}s to {end_time:.2f}s")
        convert_to_flac(input_file, output_flac, start_time, end_time)
        
        # Upload to S3
        s3_key = f"{s3_folder}{os.path.basename(output_flac)}"
        s3_uri = upload_to_s3(output_flac, s3_bucket, s3_key)
        
        # Create chunk info
        chunk_info = {
            "chunk_index": i,
            "start_time": start_time,
            "end_time": end_time,
            "path": output_flac,
            "file_size_bytes": os.path.getsize(output_flac),
            "s3_uri": s3_uri,
            "source_type": source_type,
            "source_id": source_id,
            "name": name
        }
        
        # Insert into database
        chunks.append(chunk_info)
    
    return chunks


@task
def create_transcription_chunks(file_info: Dict, max_size_mb: int = 30, overlap_seconds: int = 10) -> List[Dict]:
    """
    Create transcription chunks for a file based on size limitations, convert to FLAC,
    and upload to S3 bucket.
    
    Args:
        file_info: File information dictionary
        max_size_mb: Maximum size for Groq in MB
        overlap_seconds: Overlap between chunks in seconds
        job_id: ID of the Groq job (used for S3 subfolder)
        
    Returns:
        List of chunk dictionaries
    """
    logger = get_run_logger()
    
    if not file_info or "local_path" not in file_info:
        logger.warning("No valid file info provided")
        return []
    
    source_type = file_info["source_type"]
    source_id = file_info["source_id"]
    input_file = file_info["local_path"]
    
    
    # S3 bucket and folder configuration
    s3_bucket = "fmtt"
    s3_folder = f""
    
    # Get file size
    file_size_bytes = os.path.getsize(input_file)
    file_size_mb = file_size_bytes / (1024 * 1024)
    
    # Get duration
    duration = get_audio_duration(input_file)
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp(prefix="transcription_chunks_")
    logger.info(f"Created temporary directory: {temp_dir}")
    
    try:
        # Calculate number of chunks needed
        num_chunks = math.ceil(file_size_mb / max_size_mb)
        
        if num_chunks <= 1:
            # Process as a single chunk
            logger.info(f"File {os.path.basename(input_file)} is {file_size_mb:.2f}MB, below threshold of {max_size_mb}MB. No splitting needed.")
            return [process_single_chunk(input_file, source_type, source_id, temp_dir, s3_bucket, s3_folder)]
        else:
            # Process as multiple chunks
            logger.info(f"File size ({file_size_mb:.2f}MB) exceeds limit ({max_size_mb}MB). Splitting into {num_chunks} chunks.")
            return process_multiple_chunks(
                input_file, source_type, source_id, temp_dir, s3_bucket, s3_folder,
                num_chunks, duration, overlap_seconds, file_info
            )
    
    except Exception as e:
        logger.error(f"Error processing audio file: {e}")
        
        # Try fallback to single chunk if splitting fails
        try:
            logger.info("Attempting fallback to single chunk processing")
            return [process_single_chunk(input_file, source_type, source_id, temp_dir, s3_bucket, s3_folder)]
        except Exception as fallback_e:
            logger.error(f"Fallback processing failed: {fallback_e}")
            raise
        
    finally:
        # Clean up temporary files if needed
        # Uncomment the line below to remove temporary files
        # shutil.rmtree(temp_dir, ignore_errors=True)
        pass



