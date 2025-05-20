"""
Combined file processor for Feldenflow.

This script provides a unified entry point for processing both text and audio files
from provider drop directories.
"""

import asyncio
from prefect import flow, get_run_logger

from text_file_processor import (
    text_file_processing_flow,
    process_provider_from_name as process_text_provider
)

from audio_file_processor import (
    audio_file_processing_flow,
    process_provider_from_name as process_audio_provider
)

@flow(name="Combined File Processing Flow", description="Process both text and audio files from drop directories")
async def combined_file_processing_flow():
    """
    Main flow to process both text and audio files.
    Runs both workflows in sequence.
    """
    logger = get_run_logger()
    logger.info("Starting combined file processing flow")
    
    # Process text files first
    logger.info("Processing text files...")
    text_result = await text_file_processing_flow()
    
    # Process audio files next
    logger.info("Processing audio files...")
    audio_result = await audio_file_processing_flow()
    
    # Combine results
    return {
        "status": "completed",
        "text_files_processed": text_result.get("files_processed", 0),
        "audio_files_processed": audio_result.get("files_processed", 0),
        "text_results": text_result,
        "audio_results": audio_result
    }

async def process_provider_files(provider_name: str):
    """
    Process both text and audio files for a specific provider.
    
    Args:
        provider_name: Name of the provider
    """
    logger = get_run_logger()
    logger.info(f"Processing files for provider: {provider_name}")
    
    # Process text files first
    text_result = await process_text_provider(provider_name)
    
    # Process audio files next
    audio_result = await process_audio_provider(provider_name)
    
    # Combine results
    return {
        "status": "completed",
        "provider": provider_name,
        "text_files_processed": text_result.get("files_processed", 0),
        "audio_files_processed": audio_result.get("files_processed", 0),
        "text_results": text_result,
        "audio_results": audio_result
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process text and audio files from drop directories')
    parser.add_argument('--provider', help='Process files for a specific provider (by name)')
    parser.add_argument('--type', choices=['text', 'audio', 'all'], default='all',
                        help='Type of files to process (text, audio, or all)')
    
    args = parser.parse_args()
    
    if args.provider:
        # Process files for a specific provider
        if args.type == 'text':
            asyncio.run(process_text_provider(args.provider))
        elif args.type == 'audio':
            asyncio.run(process_audio_provider(args.provider))
        else:  # 'all'
            asyncio.run(process_provider_files(args.provider))
    else:
        # Process files for all providers
        if args.type == 'text':
            asyncio.run(text_file_processing_flow())
        elif args.type == 'audio':
            asyncio.run(audio_file_processing_flow())
        else:  # 'all'
            asyncio.run(combined_file_processing_flow())