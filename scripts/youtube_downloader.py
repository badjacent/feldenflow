from datetime import datetime, timedelta, timezone
import os
import json
import asyncio
from typing import List, Dict, Optional
import psycopg2
import yt_dlp

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

import connection  # Your connection parameters file

# Constants
MAX_CONCURRENT_DOWNLOADS = 3  # Maximum number of concurrent downloads

from dotenv import load_dotenv


load_dotenv()


@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def get_videos_to_process(limit=100):
    """
    Get videos that need to be downloaded and processed
    """
    logger = get_run_logger()
    logger.info(f"Getting up to {limit} videos to download and process")
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        # Get videos that need to be downloaded
        cursor.execute(
            """SELECT yt_video.id, video_id, yt_channel_id, yt_channel.channel_id
               FROM yt_video
               INNER JOIN yt_channel on yt_channel_id=yt_channel.id
               WHERE upload_successful = B'0' 
               AND retries_remaining > 0
               LIMIT %s""",
            (limit,)
        )
        
        videos = [
            {
                "id": row[0],
                "video_id": row[1],
                "yt_channel_id": row[3],
                "local_path": None,  # Will be populated after download
                "metadata_exists": True  # Already exists in database
            }
            for row in cursor.fetchall()
        ]
        
        logger.info(f"Found {len(videos)} videos to process")
        return videos
        
    except Exception as e:
        logger.error(f"Error getting videos to process: {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()

@task(retries=2, retry_delay_seconds=30)
async def download_video_task(video_info: Dict) -> Dict:
    """
    Download a video to disk and return updated video info
    """
    logger = get_run_logger()
    video_id = video_info["video_id"]
    db_channel_id = video_info["yt_channel_id"]
    
    logger.info(f"Processing video {video_id} for channel ID {db_channel_id}")
    output_dir = "/Volumes/T9/Feld/yt"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create channel subdirectory
    channel_dir = os.path.join(output_dir, f"{db_channel_id}")
    os.makedirs(channel_dir, exist_ok=True)
    
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    output_path = os.path.join(channel_dir, f'{video_id}.%(ext)s')
    
    # Configure yt-dlp options for download
    ydl_opts = {
        'format': 'bestaudio/best',  # Prefer audio format since we're doing speech-to-text
        'outtmpl': output_path,
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=True)
            # Get the actual filename that was saved (with extension)
            if info and 'ext' in info:
                output_filename = os.path.join(channel_dir, f'{video_id}.mp3')
                if os.path.exists(output_filename):
                    logger.info(f"Successfully downloaded video {video_id} to {output_filename}")
                    video_info["local_path"] = output_filename
                    video_info["download_success"] = True
                    
                    # Update database to mark as successful
                    update_video_download_status(video_info["id"], success=True)
                    return video_info
            
        logger.error(f"File not found after download for video {video_id}")
        video_info["download_success"] = False
        update_video_download_status(video_info["id"], success=False)
        return video_info
            
    except Exception as e:
        logger.error(f"Error downloading video {video_id}: {e}")
        video_info["download_success"] = False
        update_video_download_status(video_info["id"], success=False)
        return video_info

@task
def update_video_download_status(video_id: int, success: bool):
    """Update the database with download status"""
    logger = get_run_logger()
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        if success:
            cursor.execute(
                "UPDATE yt_video SET upload_successful = B'1' WHERE id = %s",
                (video_id,)
            )
            logger.info(f"Marked video ID {video_id} as successfully downloaded")
        else:
            cursor.execute(
                "UPDATE yt_video SET retries_remaining = retries_remaining - 1 WHERE id = %s",
                (video_id,)
            )
            logger.info(f"Decremented retry count for video ID {video_id}")
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating video download status: {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()

@task
def check_video_metadata_exists(video_id: str) -> bool:
    """Check if the video metadata already exists in the database"""
    logger = get_run_logger()
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            "SELECT id FROM yt_video WHERE video_id = %s",
            (video_id,)
        )
        exists = cursor.fetchone() is not None
        
        if exists:
            logger.info(f"Metadata for video {video_id} already exists in database")
        else:
            logger.info(f"No metadata found for video {video_id}")
            
        return exists
        
    except Exception as e:
        logger.error(f"Error checking if video exists: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()

@task
def get_private_channel_videos(db_channel_id: int) -> List[Dict]:
    """Get the list of videos for a private channel from the database"""
    logger = get_run_logger()
    logger.info(f"Getting videos for private channel ID: {db_channel_id}")
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """SELECT id, name, video_id 
               FROM yt_private_channel_video 
               WHERE yt_channel_id = %s""",
            (db_channel_id,)
        )
        
        videos = []
        for row in cursor.fetchall():
            video_id = row[2]
            # Check if this video already exists in yt_video table
            metadata_exists = check_video_metadata_exists(video_id)
            
            videos.append({
                "id": row[0],  # private_channel_video id
                "name": row[1], 
                "video_id": video_id,
                "metadata_exists": metadata_exists
            })
        
        logger.info(f"Found {len(videos)} private videos for channel {db_channel_id}")
        return videos
        
    except Exception as e:
        logger.error(f"Error getting private channel videos: {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()

@task
def extract_video_metadata(video_info: Dict, retries=3, retry_delay=5) -> Dict:
    """Extract metadata for a specific video if it doesn't already exist"""
    logger = get_run_logger()
    video_id = video_info["video_id"]
    
    # Skip if metadata already exists
    if video_info.get("metadata_exists", False):
        logger.info(f"Skipping metadata extraction for video {video_id} - already exists")
        return video_info
    
    logger.info(f"Extracting metadata for video: {video_id}")
    
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    
    # Configure yt-dlp options
    ydl_opts = {
        'skip_download': True,  # Don't download, just get metadata
        'no_playlist': True,
        'quiet': True,
        'no_warnings': True,
        'ignoreerrors': False  # We'll handle errors ourselves
    }
    
    for attempt in range(retries):
        try:
            # Create a yt-dlp instance with our options
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract info - this returns the full metadata dictionary
                metadata = ydl.extract_info(video_url, download=False)
                
                if metadata:
                    logger.info(f"Successfully extracted metadata for {video_id}")
                    video_info["metadata"] = metadata
                    return video_info
                
        except Exception as e:
            logger.error(f"Error extracting metadata for {video_id} (attempt {attempt+1}/{retries}): {str(e)}")
            
        # If we're not on the last retry, wait before trying again
        if attempt < retries - 1:
            logger.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    # If we've exhausted all retries, return without metadata
    logger.warning(f"Failed to extract metadata for {video_id} after {retries} attempts")
    return video_info

@task
def insert_video_metadata(video_info: Dict, channel_id: int) -> Dict:
    """Insert video metadata into the database if it doesn't exist yet"""
    logger = get_run_logger()
    video_id = video_info["video_id"]
    
    # Skip if metadata already exists
    if video_info.get("metadata_exists", False):
        logger.info(f"Skipping metadata insertion for video {video_id} - already exists")
        return video_info
    
    # Skip if we failed to get metadata
    if "metadata" not in video_info:
        logger.warning(f"Skipping metadata insertion for video {video_id} - no metadata available")
        return video_info
    
    metadata = video_info["metadata"]
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        dt = datetime.now(timezone.utc)
        entry_as_json = json.dumps(metadata)
        
        # Check if the video already exists
        cursor.execute(
            "SELECT id FROM yt_video WHERE video_id = %s",
            (video_id,)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Update existing entry
            cursor.execute(
                "UPDATE yt_video SET entry = %s, update_date = %s WHERE id = %s",
                (entry_as_json, dt, existing[0])
            )
            logger.info(f"Updated existing metadata for video {video_id}")
            video_info["id"] = existing[0]
        else:
            # Insert new entry
            cursor.execute(
                """INSERT INTO yt_video 
                   (yt_channel_id, video_id, upload_successful, retries_remaining, entry, create_date, update_date) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (channel_id, video_id, 'B''0''', 3, entry_as_json, dt, dt)
            )
            new_id = cursor.fetchone()[0]
            logger.info(f"Inserted new metadata for video {video_id} with ID {new_id}")
            video_info["id"] = new_id
        
        conn.commit()
        return video_info
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting video metadata: {e}")
        return video_info
        
    finally:
        cursor.close()
        conn.close()

@flow(name="Download Videos", description="Download videos from YouTube")
async def download_videos_flow(videos_to_process: List[Dict] = None, limit: int = 20) -> List[Dict]:
    """
    Flow to download multiple videos concurrently
    
    Args:
        videos_to_process: Optional list of video dictionaries to process
        limit: Maximum number of videos to fetch if videos_to_process is not provided
        
    Returns:
        List of processed video dictionaries with download status
    """
    logger = get_run_logger()
    
    # If no videos provided, fetch them from database
    if videos_to_process is None:
        videos_to_process = get_videos_to_process(limit)
        
    if not videos_to_process:
        logger.info("No videos to process, ending flow")
        return []
    
    logger.info(f"Starting download of {len(videos_to_process)} videos")
    
    # Process videos in batches to limit concurrency
    results = []
    for i in range(0, len(videos_to_process), MAX_CONCURRENT_DOWNLOADS):
        batch = videos_to_process[i:i + MAX_CONCURRENT_DOWNLOADS]
        
        # Create a list to store the download tasks
        download_tasks = []
        for video in batch:
            # Call the task function directly
            download_tasks.append(download_video_task(video))
        
        # Wait for all tasks in the batch to complete
        batch_results = await asyncio.gather(*download_tasks)
        results.extend(batch_results)
    
    # Filter to only successful downloads
    successful_downloads = [video for video in results if video.get("download_success", False)]
    logger.info(f"Successfully downloaded {len(successful_downloads)} out of {len(videos_to_process)} videos")
    
    return results

@flow(name="Process Private Channel", description="Process videos from a private YouTube channel")
async def process_private_channel_flow(channel_id: int, channel_name: str) -> List[Dict]:
    """
    Flow to process videos from a private YouTube channel
    
    Args:
        channel_id: Database ID of the channel
        channel_name: Name of the channel
        
    Returns:
        List of processed video dictionaries
    """
    logger = get_run_logger()
    logger.info(f"Processing private channel: {channel_name} (ID: {channel_id})")
    
    # Get videos for the private channel
    private_videos = get_private_channel_videos(channel_id)
    
    if not private_videos:
        logger.info(f"No videos found for private channel {channel_name}")
        return []
    
    # Process each video to extract and save metadata (if needed)
    processed_videos = []
    for video in private_videos:
        # Skip metadata extraction if it already exists
        if not video.get("metadata_exists", False):
            # Call tasks directly, not using .submit()
            video_with_metadata = extract_video_metadata(video)
            video_with_db = insert_video_metadata(video_with_metadata, channel_id)
            processed_videos.append(video_with_db)
        else:
            processed_videos.append(video)
    
    logger.info(f"Processed {len(processed_videos)} videos for private channel {channel_name}")
    return processed_videos

if __name__ == "__main__":
    # This allows the flow to be run directly
    import asyncio
    
    # Example usage:
    # asyncio.run(download_videos_flow())  # Run download flow
    # asyncio.run(process_private_channel_flow(1, "SampleChannel"))  # Process a private channel
    
    # Run the default flow
    asyncio.run(download_videos_flow())