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
from database import YTChannel, YTVideo, YTVideoEntry, YTPrivateChannelVideo

load_dotenv()


@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def get_db_channel_info(channel_id=None):
    """
    Get channel information from the database.
    If channel_id is provided, get only that channel, otherwise get all active channels.
    """
    logger = get_run_logger()
    logger.info(f"Getting channel info from database")
    
    if channel_id:
        channel = YTChannel.get_by_id(channel_id)
        return [channel] if channel else []
    else:
        return YTChannel.get_all_active()


@task(retries=3, retry_delay_seconds=5)
def get_db_channel_id(name, channel_id):
    """Get or create a channel ID in the database using Pydantic models"""
    logger = get_run_logger()
    logger.info(f"Getting/creating database ID for channel: {name}")
    
    # Check if the channel already exists
    existing_channel = None
    channels = YTChannel.get_all_active()
    for channel in channels:
        if channel.channel_id == channel_id:
            existing_channel = channel
            break
    
    if existing_channel:
        logger.info(f"Found existing channel with ID: {existing_channel.id}")
        return existing_channel.id
    else:
        # Create new channel
        new_channel = YTChannel(
            name=name,
            channel_id=channel_id,
            upload_active=True
        )
        new_channel = new_channel.save()
        logger.info(f"Created new channel with ID: {new_channel.id}")
        return new_channel.id


@task(retries=3, retry_delay_seconds=5)
def insert_unique_urls(db_channel_id, video_entries):
    """Insert or update video entries using Pydantic models"""
    logger = get_run_logger()
    logger.info(f"Inserting/updating {len(video_entries)} videos for channel ID: {db_channel_id}")
    
    # Get existing videos to check which ones need updating vs inserting
    existing_videos = {}
    videos = YTVideo.get_pending_downloads(db_channel_id)
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
                yt_channel_id=db_channel_id,
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
def load_public_channel_videos(channel_info):
    """Load videos from a public YouTube channel"""
    logger = get_run_logger()
    channel_name = channel_info.name
    db_channel_id = channel_info.id
    
    logger.info(f"Getting videos for public channel: {channel_name}")
    
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'force_generic_extractor': True,
        'skip_download': True,
        'max_downloads': 9999,  # Set high to get all videos
    }
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        # Get channel playlist URL
        channel_url = f"https://www.youtube.com/{channel_name}/videos"
        result = ydl.extract_info(channel_url, download=False, process=False)
        
        if 'entries' not in result:
            logger.warning(f"No videos found for channel {channel_name}")
            return []
        
        # Extract video entries
        video_entries = list(result['entries'])
        logger.info(f"Found {len(video_entries)} videos in channel {channel_name}")
        
        # Insert videos into database
        insert_unique_urls(db_channel_id, video_entries)
        
        return video_entries


@task(retries=3, retry_delay_seconds=5)
def get_private_channel_videos(db_channel_id):
    """Get the list of videos for a private channel from the database"""
    logger = get_run_logger()
    logger.info(f"Getting videos for private channel ID: {db_channel_id}")
    
    # Get all private videos for this channel using our model
    private_videos = YTPrivateChannelVideo.get_by_channel_id(db_channel_id)
    
    videos = [
        {
            "id": video.video_id,
            "title": video.name
        }
        for video in private_videos
    ]
    
    logger.info(f"Found {len(videos)} private videos for channel {db_channel_id}")
    return videos


@task(retries=3, retry_delay_seconds=15)
def extract_video_metadata(video_id, db_channel_id):
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
def download_video(video_id, db_channel_id, output_dir="videos"):
    """Download a video to disk"""
    logger = get_run_logger()
    logger.info(f"Downloading video {video_id} to disk")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create channel subdirectory
    channel_dir = os.path.join(output_dir, f"channel_{db_channel_id}")
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
def get_videos_to_download(db_channel_id):
    """Get videos that need to be downloaded"""
    logger = get_run_logger()
    logger.info(f"Getting videos to download for channel {db_channel_id}")
    
    # Get videos pending download using our model
    videos = YTVideo.get_pending_downloads(db_channel_id)
    
    simplified_videos = [
        {
            "id": video.id,
            "video_id": video.video_id
        }
        for video in videos
    ]
    
    logger.info(f"Found {len(simplified_videos)} videos to download for channel {db_channel_id}")
    return simplified_videos


@flow(name="Process YouTube Channel", description="Process videos from a YouTube channel")
def process_channel(channel_id=None):
    """Process a single YouTube channel or all active channels"""
    logger = get_run_logger()
    
    # Get channel info
    channels = get_db_channel_info(channel_id)
    logger.info(f"Processing {len(channels)} channels")
    
    for channel in channels:
        db_channel_id = channel.id
        is_private = channel.private_channel
        
        # Get videos based on channel type
        logger.info(f'is private: {is_private}')
        if is_private:
            logger.info(f"Processing private channel: {channel.name}")
            videos = get_private_channel_videos(db_channel_id)
            
            # For private channels, we need to extract metadata for each video
            for video in videos:
                video_id = video["id"]
                metadata = extract_video_metadata(video_id, db_channel_id)
                
                if metadata:
                    # Insert or update the video metadata
                    insert_unique_urls(db_channel_id, [metadata])
        else:
            logger.info(f"Processing public channel: {channel.name}")
            # For public channels, we get all videos and metadata in one go
            load_public_channel_videos(channel)
        
        # # Get videos that need to be downloaded
        # videos_to_download = get_videos_to_download(db_channel_id)
        
        # # Download videos
        # for video in videos_to_download:
        #     download_video(video["video_id"], db_channel_id)


@flow(name="YouTube Data Pipeline", description="Main flow for YouTube data pipeline")
def youtube_pipeline(channel_id=None):
    """Main flow for YouTube data pipeline"""
    logger = get_run_logger()
    logger.info("Starting YouTube data pipeline")
    
    # Process channels
    process_channel(channel_id)
    
    logger.info("YouTube data pipeline completed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run YouTube Metadata Scraper Pipeline')
    parser.add_argument('--channel-id', type=int, help='Channel ID to process (default: process all active channels)')
    
    args = parser.parse_args()
    
    # Run the flow with the specified channel ID or for all active channels
    youtube_pipeline(args.channel_id)