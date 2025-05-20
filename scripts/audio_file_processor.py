"""
Audio file processor for Feldenflow.

This script monitors a drop directory for audio files, processes them,
and adds them to the database for later transcription via Groq.
"""

from typing import List, Dict, Optional, Tuple
import os
import shutil
import glob
import subprocess
from datetime import datetime, timezone
import asyncio
import json

from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

load_dotenv()

from database import (
    DocumentProvider,
    AudioFile
)

# Supported audio file extensions
AUDIO_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.m4a', '.flac']

@task
def get_audio_metadata(file_path: str) -> Dict:
    """
    Get metadata from an audio file using ffprobe.
    
    Args:
        file_path: Path to the audio file
        
    Returns:
        Dictionary containing audio metadata
    """
    logger = get_run_logger()
    
    try:
        # Get duration
        duration_cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration", 
            "-of", "default=noprint_wrappers=1:nokey=1", file_path
        ]
        duration_result = subprocess.run(
            duration_cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            check=True
        )
        duration = float(duration_result.stdout.strip())
        
        # Get other metadata
        metadata_cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format_tags",
            "-of", "json", file_path
        ]
        metadata_result = subprocess.run(
            metadata_cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            check=True
        )
        
        # Parse JSON metadata if available
        try:
            metadata_json = json.loads(metadata_result.stdout)
            tags = metadata_json.get('format', {}).get('tags', {})
        except json.JSONDecodeError:
            tags = {}
        
        # Create metadata dictionary
        metadata = {
            "duration_seconds": duration,
            "tags": tags,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Extracted metadata from {file_path}: duration={duration:.2f}s")
        return metadata
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting audio metadata for {file_path}: {e}")
        # Return basic metadata if ffprobe fails
        return {
            "duration_seconds": None,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error getting audio metadata: {e}")
        return {
            "duration_seconds": None,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }

@task
def scan_provider_drop_directories() -> List[Tuple[str, DocumentProvider]]:
    """
    Scan all provider drop directories for audio files.
    
    Returns:
        List of tuples containing file path and provider instance
    """
    logger = get_run_logger()
    
    # Get storage path from environment
    storage_path = os.getenv('FILE_STORAGE_PATH')
    if not storage_path:
        logger.error("FILE_STORAGE_PATH environment variable not set")
        return []
    
    # Get all providers from database
    providers = DocumentProvider.get_all_active_youtube()  # We can use this as it gets all providers
    
    # Create drop directory if it doesn't exist
    drop_base_dir = os.path.join(storage_path, "drop")
    os.makedirs(drop_base_dir, exist_ok=True)
    
    # Create audio directory if it doesn't exist
    audio_base_dir = os.path.join(storage_path, "audio_file")
    os.makedirs(audio_base_dir, exist_ok=True)
    
    # Scan for files in each provider's drop directory
    files_to_process = []
    
    for provider in providers:
        # Use 'name' for directory name
        provider_dir = os.path.join(drop_base_dir, provider.name)
        
        # Skip if provider directory doesn't exist
        if not os.path.exists(provider_dir):
            continue
        
        # Find all audio files with supported extensions
        audio_files = []
        for ext in AUDIO_EXTENSIONS:
            audio_files.extend(glob.glob(os.path.join(provider_dir, f"*{ext}")))
            # Also check for uppercase extensions
            audio_files.extend(glob.glob(os.path.join(provider_dir, f"*{ext.upper()}")))
        
        # Add files to the list
        for file_path in audio_files:
            files_to_process.append((file_path, provider))
            logger.info(f"Found audio file to process: {file_path} for provider {provider.name}")
    
    logger.info(f"Found {len(files_to_process)} audio files to process")
    return files_to_process

@task
def process_audio_file(file_path: str, provider: DocumentProvider) -> Optional[AudioFile]:
    """
    Process an audio file:
    1. Extract metadata
    2. Copy to audio_file directory
    3. Create entry in audio_file table
    
    Args:
        file_path: Path to the audio file
        provider: Provider instance
        
    Returns:
        AudioFile instance if successful, None otherwise
    """
    logger = get_run_logger()
    
    # Get storage path from environment
    storage_path = os.getenv('FILE_STORAGE_PATH')
    if not storage_path:
        logger.error("FILE_STORAGE_PATH environment variable not set")
        return None
    
    # Get file properties
    filename = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    
    # Create audio directory for provider if it doesn't exist
    audio_dir = os.path.join(storage_path, "audio_file", provider.name)
    os.makedirs(audio_dir, exist_ok=True)
    
    # Target file path
    target_path = os.path.join(audio_dir, filename)
    
    try:
        # Get audio metadata
        metadata = get_audio_metadata(file_path)
        duration_seconds = metadata.get("duration_seconds")
        
        # Copy file to audio directory
        shutil.copy2(file_path, target_path)
        logger.info(f"Copied {file_path} to {target_path}")
        
        # Create database entry
        audio_file = AudioFile(
            file_path=target_path,
            original_filename=filename,
            file_size_bytes=file_size,
            duration_seconds=duration_seconds,
            source_type="audio_file",
            metadata=metadata
        )
        audio_file = audio_file.save()
        
        logger.info(f"Created audio file entry with ID {audio_file.id}")
        
        # Delete the original file from drop directory
        os.remove(file_path)
        logger.info(f"Deleted original file {file_path}")
        
        return audio_file
    
    except Exception as e:
        logger.error(f"Error processing audio file {file_path}: {e}")
        return None

@flow(name="Audio File Processing Flow", description="Process audio files from drop directories")
async def audio_file_processing_flow():
    """
    Main flow to process audio files:
    1. Scan for audio files in provider drop directories
    2. Process each file (extract metadata, copy, create database entry)
    """
    logger = get_run_logger()
    logger.info("Starting audio file processing flow")
    
    # Scan for files
    files_to_process = scan_provider_drop_directories()
    
    if not files_to_process:
        logger.info("No audio files found to process")
        return {"status": "completed", "files_processed": 0}
    
    # Process each file
    processed_files = []
    
    for file_path, provider in files_to_process:
        # Process the file
        audio_file = process_audio_file(file_path, provider)
        
        if audio_file:
            processed_files.append({
                "audio_file_id": audio_file.id,
                "provider_id": provider.id,
                "provider_name": provider.name,
                "filename": audio_file.original_filename,
                "duration": audio_file.duration_seconds
            })
    
    logger.info(f"Audio file processing completed. Processed {len(processed_files)} files.")
    
    return {
        "status": "completed",
        "files_processed": len(processed_files),
        "processed_files": processed_files
    }

async def process_provider_from_name(provider_name: str) -> Dict:
    """
    Process a specific provider based on name.
    
    Args:
        provider_name: Name of the provider
        
    Returns:
        Status dictionary
    """
    logger = get_run_logger()
    
    # Find provider by name
    with DocumentProvider.get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM document_provider WHERE name = %s",
                (provider_name,)
            )
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"Provider with name '{provider_name}' not found")
                return {"status": "error", "message": f"Provider not found: {provider_name}"}
            
            provider_id = result[0]
    
    # Get provider
    provider = DocumentProvider.get_by_id(provider_id)
    
    # Process provider
    logger.info(f"Processing provider {provider.name} (ID: {provider.id})")
    
    # Get storage path from environment
    storage_path = os.getenv('FILE_STORAGE_PATH')
    if not storage_path:
        logger.error("FILE_STORAGE_PATH environment variable not set")
        return {"status": "error", "message": "FILE_STORAGE_PATH environment variable not set"}
    
    # Create drop directory if it doesn't exist
    drop_dir = os.path.join(storage_path, "drop", provider.name)
    
    # Skip if provider directory doesn't exist
    if not os.path.exists(drop_dir):
        logger.warning(f"Drop directory for provider {provider.name} does not exist: {drop_dir}")
        return {"status": "warning", "message": f"Drop directory does not exist: {drop_dir}"}
    
    # Find all audio files
    audio_files = []
    for ext in AUDIO_EXTENSIONS:
        audio_files.extend(glob.glob(os.path.join(drop_dir, f"*{ext}")))
        audio_files.extend(glob.glob(os.path.join(drop_dir, f"*{ext.upper()}")))
    
    # Process each file
    processed_files = []
    
    for file_path in audio_files:
        audio_file = process_audio_file(file_path, provider)
        
        if audio_file:
            processed_files.append({
                "audio_file_id": audio_file.id,
                "provider_id": provider.id,
                "provider_name": provider.name,
                "filename": audio_file.original_filename,
                "duration": audio_file.duration_seconds
            })
    
    logger.info(f"Processed {len(processed_files)} audio files for provider {provider.name}")
    
    return {
        "status": "completed",
        "provider": provider.name,
        "files_processed": len(processed_files),
        "processed_files": processed_files
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process audio files from drop directories')
    parser.add_argument('--provider', help='Process files for a specific provider (by name)')
    
    args = parser.parse_args()
    
    if args.provider:
        asyncio.run(process_provider_from_name(args.provider))
    else:
        asyncio.run(audio_file_processing_flow())