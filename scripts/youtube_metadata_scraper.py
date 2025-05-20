from datetime import datetime, timedelta, timezone
import os
import json
from pathlib import Path
import time

# External libraries
import yt_dlp
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from dotenv import load_dotenv

# Import database models
from database import DocumentProvider, YTVideo, YTVideoEntry, YTPrivateChannelVideo

load_dotenv()


@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def get_db_provider_info(provider_id=None):
    """
    Get provider information from the database.
    If provider_id is provided, get only that provider, otherwise get all active YouTube providers.
    """
    logger = get_run_logger()
    logger.info(f"Getting provider info from database")
    
    if provider_id:
        provider = DocumentProvider.get_by_id(provider_id)
        return [provider] if provider else []
    else:
        return DocumentProvider.get_all_active_youtube()


@task(retries=3, retry_delay_seconds=5)
def get_db_provider_id(name, channel_id):
    """Get or create a provider ID in the database using Pydantic models"""
    logger = get_run_logger()
    logger.info(f"Getting/creating database ID for provider: {name}")
    
    # Check if the provider already exists
    existing_provider = None
    providers = DocumentProvider.get_all_active_youtube()
    for provider in providers:
        if provider.yt_channel_id == channel_id:
            existing_provider = provider
            break
    
    if existing_provider:
        logger.info(f"Found existing provider with ID: {existing_provider.id}")
        return existing_provider.id
    else:
        # Create new provider
        new_provider = DocumentProvider(
            name=name,
            yt_name=name,
            yt_channel_id=channel_id,
            yt_upload_active=True
        )
        new_provider = new_provider.save()
        logger.info(f"Created new provider with ID: {new_provider.id}")
        return new_provider.id


@task(retries=3, retry_delay_seconds=5)
def insert_unique_urls(db_provider_id, video_entries):
    """Insert or update video entries using Pydantic models"""
    logger = get_run_logger()
    logger.info(f"Inserting/updating {len(video_entries)} videos for provider ID: {db_provider_id}")
    
    # Get existing videos to check which ones need updating vs inserting
    existing_videos = {}
    videos = YTVideo.get_pending_downloads(db_provider_id)
    for video in videos:
        existing_videos[video.video_id] = video
    
    inserted_count = 0
    updated_count = 0
    
    # For each entry in the list
    for entry in video_entries:
        url = entry['id']
        
        # Check if the URL already exists
        if url in existing_videos:
            # Update if entry has changed
            existing_video = existing_videos[url]
            new_entry = YTVideoEntry(**entry)
            
            if existing_video.entry.dict() != new_entry.dict():
                existing_video.entry = new_entry
                existing_video.update_date = datetime.now(timezone.utc)
                existing_video.save()
                updated_count += 1
                logger.info(f"Updated video entry for {url}")
        else:
            # Insert new video
            new_video = YTVideo(
                document_provider_id=db_provider_id,
                video_id=url,
                upload_successful=False,
                retries_remaining=3,
                entry=YTVideoEntry(**entry),
                create_date=datetime.now(timezone.utc),
                update_date=datetime.now(timezone.utc)
            )
            new_video.save()
            inserted_count += 1
            logger.info(f"Inserted new video entry for {url}")
    
    logger.info(f"Inserted {inserted_count} new videos, updated {updated_count} existing videos")
    return inserted_count + updated_count


@task(retries=3, retry_delay_seconds=10)
def load_public_channel_videos(provider_info):
    """Load videos from a public YouTube channel"""
    logger = get_run_logger()
    provider_name = provider_info.yt_name or provider_info.name
    db_provider_id = provider_info.id
    
    logger.info(f"Getting videos for public channel: {provider_name}")
    
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'force_generic_extractor': True,
        'skip_download': True,
        'max_downloads': 9999,  # Set high to get all videos
    }
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        # Get channel playlist URL
        channel_url = f"https://www.youtube.com/{provider_name}/videos"
        result = ydl.extract_info(channel_url, download=False, process=False)
        
        if 'entries' not in result:
            logger.warning(f"No videos found for channel {provider_name}")
            return []
        
        # Extract video entries
        video_entries = list(result['entries'])
        logger.info(f"Found {len(video_entries)} videos in channel {provider_name}")
        
        # Insert videos into database
        insert_unique_urls(db_provider_id, video_entries)
        
        return video_entries


@task(retries=3, retry_delay_seconds=5)
def get_private_channel_videos(db_provider_id):
    """Get the list of videos for a private provider from the database"""
    logger = get_run_logger()
    logger.info(f"Getting videos for private provider ID: {db_provider_id}")
    
    # Get all private videos for this provider using our model
    private_videos = YTPrivateChannelVideo.get_by_provider_id(db_provider_id)
    
    videos = [
        {
            "id": video.video_id,
            "title": video.name
        }
        for video in private_videos
    ]
    
    logger.info(f"Found {len(videos)} private videos for provider {db_provider_id}")
    return videos


@task(retries=3, retry_delay_seconds=15)
def extract_video_metadata(video_id, db_provider_id):
    """Extract metadata for a specific video"""
    logger = get_run_logger()
    logger.info(f"Extracting metadata for video: {video_id}")
    
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    
    # Configure yt-dlp options
    ydl_opts = {
        'skip_download': True,
        'no_playlist': True,
        'quiet': True,
        'no_warnings': True,
        'ignoreerrors': False  # We'll handle errors ourselves
    }
    
    for attempt in range(3):  # 3 retries
        try:
            # Create a yt-dlp instance with our options
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract info - this returns the full metadata dictionary
                metadata = ydl.extract_info(video_url, download=False, process=False)
                
                if metadata:
                    logger.info(f"Successfully extracted metadata for {video_id}")
                    return metadata
                
        except yt_dlp.utils.DownloadError as e:
            logger.error(f"yt-dlp download error for {video_id}: {str(e)}")
            
        except yt_dlp.utils.ExtractorError as e:
            logger.error(f"yt-dlp extractor error for {video_id}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Unexpected error for {video_id}: {str(e)}")
        
        # If we're not on the last retry, wait before trying again
        if attempt < 2:  # 3 retries - 0, 1, 2
            logger.info(f"Retrying in 5 seconds (attempt {attempt + 1}/3)...")
            time.sleep(5)
    
    # If we've exhausted all retries, return None
    logger.warning(f"Failed to extract metadata for {video_id} after 3 attempts")
    return None


@task(retries=2, retry_delay_seconds=30)
def download_video(video_id, db_provider_id, output_dir="videos"):
    """Download a video to disk"""
    logger = get_run_logger()
    logger.info(f"Downloading video {video_id} to disk")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get provider info to get channel ID for directory structure
    provider = DocumentProvider.get_by_id(db_provider_id)
    if not provider or not provider.yt_channel_id:
        logger.error(f"Provider {db_provider_id} not found or has no channel ID")
        return False
    
    # Create channel subdirectory
    channel_dir = os.path.join(output_dir, provider.yt_channel_id)
    os.makedirs(channel_dir, exist_ok=True)
    
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    
    # Configure yt-dlp options for download
    ydl_opts = {
        'format': 'best',  # Download best quality
        'outtmpl': os.path.join(channel_dir, f'{video_id}.%(ext)s'),
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
            
        # Get the video from the database and update it
        video = YTVideo.get_by_video_id(video_id)
        if video:
            video.upload_successful = True
            video.save()
            logger.info(f"Successfully downloaded video {video_id} and updated database")
            return True
        else:
            logger.error(f"Video {video_id} not found in database")
            return False
            
    except Exception as e:
        logger.error(f"Error downloading video {video_id}: {e}")
        
        # Update retry count in database
        video = YTVideo.get_by_video_id(video_id)
        if video:
            video.retries_remaining -= 1
            video.save()
            
        return False


@task
def get_videos_to_download(db_provider_id):
    """Get videos that need to be downloaded"""
    logger = get_run_logger()
    logger.info(f"Getting videos to download for provider {db_provider_id}")
    
    # Get videos pending download using our model
    videos = YTVideo.get_pending_downloads(db_provider_id)
    
    simplified_videos = [
        {
            "id": video.id,
            "video_id": video.video_id
        }
        for video in videos
    ]
    
    logger.info(f"Found {len(simplified_videos)} videos to download for provider {db_provider_id}")
    return simplified_videos


@flow(name="Process YouTube Channel", description="Process videos from a YouTube channel")
def process_channel(provider_id=None):
    """Process a single YouTube provider or all active providers"""
    logger = get_run_logger()
    
    # Get provider info
    providers = get_db_provider_info(provider_id)
    logger.info(f"Processing {len(providers)} providers")
    
    for provider in providers:
        db_provider_id = provider.id
        is_private = provider.yt_private_channel
        
        # Get videos based on provider type
        logger.info(f'is private: {is_private}')
        if is_private:
            logger.info(f"Processing private provider: {provider.name}")
            videos = get_private_channel_videos(db_provider_id)
            
            # For private channels, we need to extract metadata for each video
            for video in videos:
                video_id = video["id"]
                metadata = extract_video_metadata(video_id, db_provider_id)
                
                if metadata:
                    # Insert or update the video metadata
                    insert_unique_urls(db_provider_id, [metadata])
        else:
            logger.info(f"Processing public provider: {provider.name}")
            # For public channels, we get all videos and metadata in one go
            load_public_channel_videos(provider)
        
        # # Get videos that need to be downloaded
        # videos_to_download = get_videos_to_download(db_provider_id)
        
        # # Download videos
        # for video in videos_to_download:
        #     download_video(video["video_id"], db_provider_id)


@flow(name="YouTube Data Pipeline", description="Main flow for YouTube data pipeline")
def youtube_pipeline(provider_id=None):
    """Main flow for YouTube data pipeline"""
    logger = get_run_logger()
    logger.info("Starting YouTube data pipeline")
    
    # Process providers
    process_channel(provider_id)
    
    logger.info("YouTube data pipeline completed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run YouTube Metadata Scraper Pipeline')
    parser.add_argument('--provider-id', type=int, help='Provider ID to process (default: process all active providers)')
    
    args = parser.parse_args()
    
    # Run the flow with the specified provider ID or for all active providers
    youtube_pipeline(args.provider_id)