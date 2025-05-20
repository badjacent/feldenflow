"""
Text file processor for Feldenflow.

This script monitors a drop directory for text files, processes them,
and adds them to the database.
"""

from typing import List, Dict, Optional, Tuple
import os
import shutil
import glob
from datetime import datetime, timezone
import asyncio

from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

load_dotenv()

from database import (
    DocumentProvider,
    TextField,
    Transcription
)

@task
def scan_provider_drop_directories() -> List[Tuple[str, DocumentProvider]]:
    """
    Scan all provider drop directories for text files.
    
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
    
    # Create text directory if it doesn't exist
    text_base_dir = os.path.join(storage_path, "text")
    os.makedirs(text_base_dir, exist_ok=True)
    
    # Scan for files in each provider's drop directory
    files_to_process = []
    
    for provider in providers:
        # Use 'name' for directory instead of yt_name
        provider_dir = os.path.join(drop_base_dir, provider.name)
        
        # Skip if provider directory doesn't exist
        if not os.path.exists(provider_dir):
            continue
        
        # Find all text files (txt, md, etc.)
        text_files = glob.glob(os.path.join(provider_dir, "*.txt"))
        text_files.extend(glob.glob(os.path.join(provider_dir, "*.md")))
        
        # Add files to the list
        for file_path in text_files:
            files_to_process.append((file_path, provider))
            logger.info(f"Found text file to process: {file_path} for provider {provider.name}")
    
    logger.info(f"Found {len(files_to_process)} text files to process")
    return files_to_process

@task
def process_text_file(file_path: str, provider: DocumentProvider) -> Optional[TextField]:
    """
    Process a text file:
    1. Copy to text directory
    2. Create entry in text_file table
    
    Args:
        file_path: Path to the text file
        provider: Provider instance
        
    Returns:
        TextField instance if successful, None otherwise
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
    
    # Create text directory for provider if it doesn't exist
    text_dir = os.path.join(storage_path, "text", provider.name)
    os.makedirs(text_dir, exist_ok=True)
    
    # Target file path
    target_path = os.path.join(text_dir, filename)
    
    try:
        # Copy file to text directory
        shutil.copy2(file_path, target_path)
        logger.info(f"Copied {file_path} to {target_path}")
        
        # Create database entry
        text_file = TextField(
            document_provider_id=provider.id,
            file_path=target_path,
            original_filename=filename,
            file_size_bytes=file_size,
            metadata={"processed_at": datetime.now(timezone.utc).isoformat()}
        )
        text_file = text_file.save()
        
        logger.info(f"Created text file entry with ID {text_file.id}")
        
        # Delete the original file from drop directory
        os.remove(file_path)
        logger.info(f"Deleted original file {file_path}")
        
        return text_file
    
    except Exception as e:
        logger.error(f"Error processing text file {file_path}: {e}")
        return None

@task
def create_transcription(text_file: TextField) -> Optional[Transcription]:
    """
    Create a transcription entry for a text file.
    
    Args:
        text_file: TextField instance
        
    Returns:
        Transcription instance if successful, None otherwise
    """
    logger = get_run_logger()
    
    try:
        # Read file content
        with open(text_file.file_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
        
        # Create transcription
        transcription = Transcription(
            source_type="text_file",
            source_id=text_file.id,
            transcription=file_content
        )
        transcription = transcription.save()
        
        logger.info(f"Created transcription with ID {transcription.id} for text file {text_file.id}")
        return transcription
    
    except Exception as e:
        logger.error(f"Error creating transcription for text file {text_file.id}: {e}")
        return None

@flow(name="Text File Processing Flow", description="Process text files from drop directories")
async def text_file_processing_flow():
    """
    Main flow to process text files:
    1. Scan for text files in provider drop directories
    2. Process each file (copy, create database entry)
    3. Create transcription for each file
    """
    logger = get_run_logger()
    logger.info("Starting text file processing flow")
    
    # Scan for files
    files_to_process = scan_provider_drop_directories()
    
    if not files_to_process:
        logger.info("No text files found to process")
        return {"status": "completed", "files_processed": 0}
    
    # Process each file
    processed_files = []
    
    for file_path, provider in files_to_process:
        # Process the file
        text_file = process_text_file(file_path, provider)
        
        if text_file:
            # Create transcription
            transcription = create_transcription(text_file)
            
            if transcription:
                processed_files.append({
                    "text_file_id": text_file.id,
                    "transcription_id": transcription.id,
                    "provider_id": provider.id,
                    "provider_name": provider.name,
                    "filename": text_file.original_filename
                })
    
    logger.info(f"Text file processing completed. Processed {len(processed_files)} files.")
    
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
    
    # Rest of the implementation...
    
    return {"status": "not_implemented"}

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process text files from drop directories')
    parser.add_argument('--provider', help='Process files for a specific provider (by name)')
    
    args = parser.parse_args()
    
    if args.provider:
        asyncio.run(process_provider_from_name(args.provider))
    else:
        asyncio.run(text_file_processing_flow())