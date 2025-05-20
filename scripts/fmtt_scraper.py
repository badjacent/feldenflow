import os
import time
import json
import re
import logging
from typing import List, Dict, Optional

from prefect import flow, task
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager

from dotenv import load_dotenv

# Import database models
from database import DocumentProvider, YTPrivateChannelVideo

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def create_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Create Chrome WebDriver with detailed configuration
    
    :param headless: Run in headless mode
    :return: Configured Chrome WebDriver
    """
    try:
        options = Options()
        if headless:
            options.add_argument("--headless=new")
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36")
        
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), 
            options=options
        )
        
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
    
    except Exception as e:
        logger.error(f"Failed to create WebDriver: {e}")
        raise

@task(retries=2, retry_delay_seconds=10, persist_result=False)
def extract_fmtt_videos(username: str, password: str, starting_url: str) -> List[Dict[str, any]]:
    """
    Comprehensive task to extract YouTube videos from FMTT lessons
    
    :param username: Website login username
    :param password: Website login password
    :param starting_url: Initial URL to start lesson extraction
    :return: List of dictionaries containing lesson videos
    """
    driver = None
    try:
        # Create driver and login
        driver = create_driver()
        
        # Perform login
        driver.get("https://feldenkraismanhattantraining.com/login/")
        
        # Wait for login elements
        wait = WebDriverWait(driver, 10)
        username_field = wait.until(EC.element_to_be_clickable((By.ID, "user_login")))
        password_field = wait.until(EC.element_to_be_clickable((By.ID, "user_pass")))
        submit_button = wait.until(EC.element_to_be_clickable((By.ID, "wp-submit")))
        
        # Perform login
        username_field.clear()
        username_field.send_keys(username)
        password_field.clear()
        password_field.send_keys(password)
        submit_button.click()
        
        # Wait for page transition
        time.sleep(5)
        
        # Navigate to starting URL and find lesson links
        driver.get(starting_url)
        wait.until(EC.presence_of_element_located((By.ID, "mpcs-sidebar")))
        
        # Find lesson links in sidebar
        lesson_elements = driver.find_elements(By.CSS_SELECTOR, "div.mpcs-lesson a.mpcs-lesson-row-link")
        
        lesson_links = []
        for element in lesson_elements:
            href = element.get_attribute("href")
            # Only include links that match our pattern for lessons
            if re.search(r'day-\d+-\d+[-/]\d+[-/]\d+-chapter-\d+', href, re.IGNORECASE) or \
               re.search(r'segment-\d+-day-\d+-\d+[-/]\d+[-/]\d+-chapter-\d+', href, re.IGNORECASE):
                lesson_links.append(href)
        
        logger.info(f"Found {len(lesson_links)} lesson links")
        
        # Extract videos from each lesson
        all_videos = []
        for lesson_url in lesson_links:
            try:
                # Navigate to lesson page
                driver.get(lesson_url)
                time.sleep(3)  # Page load buffer
                
                # Get page title
                try:
                    title = driver.find_element(By.CLASS_NAME, "entry-title").text.strip()
                except NoSuchElementException:
                    title = "Unknown"
                
                # Extract HTML content
                html_content = driver.page_source
                
                # YouTube link patterns
                youtube_patterns = [
                    r'https?://(?:www\.)?youtube\.com/watch\?v=([^&\s"\']+)',
                    r'https?://(?:www\.)?youtu\.be/([^\s"\']+)',
                    r'https?://(?:www\.)?youtube\.com/embed/([^\s"\']+)'
                ]
                
                # Collect unique video IDs
                video_ids = []
                seen_ids = set()
                
                for pattern in youtube_patterns:
                    matches = re.findall(pattern, html_content)
                    for match in matches:
                        # Extract clean video ID
                        video_id = match.split('?')[0]
                        
                        # Validate and deduplicate
                        if video_id and video_id not in seen_ids:
                            seen_ids.add(video_id)
                            video_ids.append(video_id)
                
                # Add to results if video IDs found
                if video_ids:
                    all_videos.append({
                        "title": title,
                        "video_ids": video_ids
                    })
            
            except Exception as lesson_error:
                logger.error(f"Error processing lesson {lesson_url}: {lesson_error}")
        
        return all_videos
    
    except Exception as e:
        logger.error(f"Comprehensive extraction error: {e}")
        return []
    
    finally:
        if driver:
            driver.quit()

def save_videos_to_database(videos: List[Dict[str, str]], provider_id: int) -> int:
    """
    Save video IDs to PostgreSQL using Pydantic models, avoiding duplicates
    
    :param videos: List of video dictionaries
    :param provider_id: Document provider ID in database
    :return: Number of videos inserted
    """
    if not videos:
        return 0
    
    try:
        # Get the provider
        provider = DocumentProvider.get_by_id(provider_id)
        if not provider:
            logger.error(f"Provider with ID {provider_id} not found")
            return 0
        
        # Get existing videos to avoid duplicates
        existing_videos = YTPrivateChannelVideo.get_by_provider_id(provider_id)
        existing_video_ids = {video.video_id for video in existing_videos}
        
        inserted_count = 0
        
        # Insert videos using Pydantic models
        for video in videos:
            for video_id in video['video_ids']:
                # Skip if already exists
                if video_id in existing_video_ids:
                    continue
                
                # Create and save video
                private_video = YTPrivateChannelVideo(
                    name=video['title'],
                    video_id=video_id,
                    document_provider_id=provider_id
                )
                private_video.save()
                inserted_count += 1
                existing_video_ids.add(video_id)  # Update to avoid duplicates in this batch
        
        return inserted_count
    
    except Exception as e:
        logger.error(f"Database insertion error: {e}")
        return 0

@flow(log_prints=True, persist_result=False)
def fmtt_video_scraper(
    username: Optional[str] = None, 
    password: Optional[str] = None,
    provider_id: int = 3,  # Default FMTT provider ID
    starting_url: str = "https://feldenkraismanhattantraining.com/courses/nyc-training-program/lessons/day-1-2-7-25-chapter-2/"
):
    """
    Main Prefect flow for FMTT video scraping
    """
    # Use environment variables as fallback
    username = username or os.getenv('FMTT_USERNAME')
    password = password or os.getenv('FMTT_PASSWORD')
    
    # Validate inputs
    if not all([username, password]):
        logger.error("Missing credentials. Provide via arguments or environment variables.")
        raise ValueError("Missing credentials. Provide via arguments or environment variables.")
    
    # Extract videos
    all_videos = extract_fmtt_videos(username, password, starting_url)
    
    if not all_videos:
        logger.error("No videos found")
        return None
    
    # Save to database using the new models
    inserted_count = save_videos_to_database(all_videos, provider_id)
    
    return {
        "total_lesson_links": len(all_videos),
        "total_videos_found": sum(len(video['video_ids']) for video in all_videos),
        "videos_inserted": inserted_count
    }


# Deployment and scheduling
if __name__ == "__main__":
    # Run the flow
    result = fmtt_video_scraper()
    print(result)