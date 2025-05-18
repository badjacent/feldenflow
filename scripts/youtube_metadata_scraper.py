from datetime import datetime, timedelta, timezone
import os
import json
from pathlib import Path
import time

# External libraries
import yt_dlp
import psycopg2
import connection  # Your connection parameters file

# Prefect imports
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from dotenv import load_dotenv


load_dotenv()


@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def get_db_channel_info(channel_id=None):
    """
    Get channel information from the database.
    If channel_id is provided, get only that channel, otherwise get all active channels.
    """
    logger = get_run_logger()
    logger.info(f"Getting channel info from database")
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')

    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        if channel_id:
            cursor.execute(
                """SELECT id, name, channel_id, private_channel 
                   FROM yt_channel 
                   WHERE id = %s AND upload_active = B'1'""",
                (channel_id,)
            )
        else:
            cursor.execute(
                """SELECT id, name, channel_id, private_channel 
                   FROM yt_channel 
                   WHERE upload_active = B'1'"""
            )
        
        all_rows = cursor.fetchall()

        # # Log each row's private_channel value
        # for row in all_rows:
        #     logger.info(f"Channel {row[1]} (ID: {row[0]}) has private_channel value: {row[3]}, type: {type(row[3])}")

        # Then create your channels list
        channels = [
            {
                "db_channel_id": row[0],
                "name": row[1],
                "channel_id": row[2],
                "is_private": row[3] == "1"  # Convert bit to boolean
            }
            for row in all_rows
        ]

        
        return channels
        
    except Exception as e:
        logger.error(f"Error getting channel info: {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()

@task(retries=3, retry_delay_seconds=5)
def get_db_channel_id(name, channel_id):
    """Get or create a channel ID in the database"""
    logger = get_run_logger()
    logger.info(f"Getting/creating database ID for channel: {name}")
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')

    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        # Check if the channel already exists
        cursor.execute(
            "SELECT id FROM yt_channel WHERE channel_id = %s",
            (channel_id,)
        )
        existing = cursor.fetchone()
        
        # If channel doesn't exist, insert it
        if not existing:
            cursor.execute(
                "INSERT INTO yt_channel (name, channel_id, upload_active) VALUES (%s, %s, %s) RETURNING id",
                (name, channel_id, 'B''1''')
            )
            id_value = cursor.fetchone()[0]
            logger.info(f"Created new channel with ID: {id_value}")
        else:
            id_value = existing[0]
            logger.info(f"Found existing channel with ID: {id_value}")
        
        # Commit the transaction
        conn.commit()
        
    except Exception as e:
        # Roll back in case of error
        conn.rollback()
        logger.error(f"Error in get_db_channel_id: {e}")
        raise
    
    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()
    
    return id_value

@task(retries=3, retry_delay_seconds=5)
def insert_unique_urls(db_channel_id, video_entries):
    """Insert or update video entries in the yt_video table"""
    logger = get_run_logger()
    logger.info(f"Inserting/updating {len(video_entries)} videos for channel ID: {db_channel_id}")
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')

    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        # For each entry in the list
        for entry in video_entries:
            dt = datetime.now(timezone.utc) 
            url = entry['id']
            entry_as_json = json.dumps(entry)

            # Check if the URL already exists
            cursor.execute(
                "SELECT id, entry FROM yt_video WHERE video_id = %s",
                (url,)
            )
            existing = cursor.fetchone()

            if existing:
                old_entry = existing[1]
                if old_entry != entry_as_json:
                    cursor.execute(
                        "UPDATE yt_video SET entry=%s, update_date=%s WHERE id=%s",
                        (entry_as_json, dt, existing[0])
                    )
                    logger.info(f"Updated video entry for {url}")
            else:
                cursor.execute(
                    """INSERT INTO yt_video 
                       (yt_channel_id, video_id, upload_successful, retries_remaining, entry, create_date, update_date) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                    (db_channel_id, url, 'B''0''', 3, entry_as_json, dt, dt)
                )
                logger.info(f"Inserted new video entry for {url}")
        
        # Commit the transaction
        conn.commit()
        
    except Exception as e:
        # Roll back in case of error
        conn.rollback()
        logger.error(f"Error in insert_unique_urls: {e}")
        raise
    
    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()

@task(retries=3, retry_delay_seconds=10)
def load_public_channel_videos(channel_info):
    """Load videos from a public YouTube channel"""
    logger = get_run_logger()
    channel_name = channel_info["name"]
    db_channel_id = channel_info["db_channel_id"]
    
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
        
        videos = [
            {
                "id": row[2],  # video_id
                "title": row[1]  # name
            }
            for row in cursor.fetchall()
        ]
        
        logger.info(f"Found {len(videos)} private videos for channel {db_channel_id}")
        return videos
        
    except Exception as e:
        logger.error(f"Error getting private channel videos: {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()

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
            
        # Update database to mark as successful
        db_connection_string =  os.getenv('DATABASE_URL')

        conn = psycopg2.connect(db_connection_string)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "UPDATE yt_video SET upload_successful = B'1' WHERE video_id = %s",
                (video_id,)
            )
            conn.commit()
            logger.info(f"Successfully downloaded video {video_id} and updated database")
            return True
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating database after download: {e}")
            raise
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error downloading video {video_id}: {e}")
        
        # Update retry count in database
        db_connection_string =  os.getenv('DATABASE_URL')

        conn = psycopg2.connect(db_connection_string)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "UPDATE yt_video SET retries_remaining = retries_remaining - 1 WHERE video_id = %s",
                (video_id,)
            )
            conn.commit()
            
        except Exception as db_err:
            conn.rollback()
            logger.error(f"Error updating retry count: {db_err}")
            
        finally:
            cursor.close()
            conn.close()
            
        return False

@task
def get_videos_to_download(db_channel_id):
    """Get videos that need to be downloaded"""
    logger = get_run_logger()
    logger.info(f"Getting videos to download for channel {db_channel_id}")
    
    # Connect to PostgreSQL
    db_connection_string =  os.getenv('DATABASE_URL')

    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """SELECT id, video_id 
               FROM yt_video 
               WHERE yt_channel_id = %s 
               AND upload_successful = B'0' 
               AND retries_remaining > 0""",
            (db_channel_id,)
        )
        
        videos = [
            {
                "id": row[0],
                "video_id": row[1]
            }
            for row in cursor.fetchall()
        ]
        
        logger.info(f"Found {len(videos)} videos to download for channel {db_channel_id}")
        return videos
        
    except Exception as e:
        logger.error(f"Error getting videos to download: {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()

@flow(name="Process YouTube Channel", description="Process videos from a YouTube channel")
def process_channel(channel_id=None):
    """Process a single YouTube channel or all active channels"""
    logger = get_run_logger()
    
    # Get channel info
    channels = get_db_channel_info(channel_id)
    logger.info(f"Processing {len(channels)} channels")
    
    for channel in channels:
        db_channel_id = channel["db_channel_id"]
        is_private = channel["is_private"]
        
        # Get videos based on channel type
        logger.info(f'is private: {is_private}')
        if is_private:
            logger.info(f"Processing private channel: {channel['name']}")
            videos = get_private_channel_videos(db_channel_id)
            
            # For private channels, we need to extract metadata for each video
            for video in videos:
                video_id = video["id"]
                metadata = extract_video_metadata(video_id, db_channel_id)
                
                if metadata:
                    # Insert or update the video metadata
                    insert_unique_urls(db_channel_id, [metadata])
        else:
            logger.info(f"Processing public channel: {channel['name']}")
            # For public channels, we get all videos and metadata in one go
            load_public_channel_videos(channel)
        
        # # Get videos that need to be downloaded
        # videos_to_download = get_videos_to_download(db_channel_id)
        
        # # Download videos
        # for video in videos_to_download:
        #     download_video(video["video_id"], db_channel_id)

@flow(name="YouTube Data Pipeline", description="Main flow for YouTube data pipeline")
def youtube_pipeline(channel_id=2):
    """Main flow for YouTube data pipeline"""
    logger = get_run_logger()
    logger.info("Starting YouTube data pipeline")
    
    # Process channels
    process_channel(channel_id)
    
    logger.info("YouTube data pipeline completed")

if __name__ == "__main__":
    # Run the flow for all active channels
    youtube_pipeline()