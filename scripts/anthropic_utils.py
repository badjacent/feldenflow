import anthropic
import difflib
from typing import List
from dotenv import load_dotenv
import os

load_dotenv()


def merge_multiple_transcriptions(transcriptions: List[str]):
    """
    Merge multiple text transcriptions that may have overlapping sections.
    
    Args:
        transcriptions: List of text transcriptions in sequential order
    
    Returns:
        A single merged transcription
    """
    if not transcriptions:
        return ""
    
    if len(transcriptions) == 1:
        return transcriptions[0]
    
    # Initialize the merged text with the first transcription
    merged_text = transcriptions[0]
    
    # Initialize Claude client
    client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_KEY'))
    
    # Merge each subsequent transcription
    for i in range(1, len(transcriptions)):
        current_text = transcriptions[i]
        
        # Use Claude to merge the current merged text with the next transcription
        prompt = f"""
        I have two consecutive text transcriptions with some overlap. Please merge them into a single coherent transcript.
        
        TRANSCRIPTION 1 (EARLIER SECTION):
        {merged_text}
        
        TRANSCRIPTION 2 (LATER SECTION):
        {current_text}
        
        Please analyze these carefully, looking for similar phrases or sentences that indicate the overlap (approximately 10 seconds worth of text). 
        Create a single merged transcript that eliminates duplicated content and provides a natural reading experience. Pay attention to:
        - Speaker attributions if present
        - Incomplete sentences that may be completed in the other transcript
        - Similar phrasings that might be the same content transcribed slightly differently
        - Filler words or hesitations that might be transcribed inconsistently
        
        Return only the final merged transcript without any explanations.
        """
        
        # Query Claude

        print("going to anthropic")
        message = client.messages.create(
            model="claude-3-7-sonnet-20250219",
            max_tokens=4000,
            temperature=0,
            system="You are a helpful assistant specializing in text analysis and merging transcriptions.",
            messages=[{"role": "user", "content": prompt}]
        )
        print("returned from anthropic")

        
        # Update the merged text
        merged_text = message.content[0].text
        print(merged_text)

    
    return merged_text

def optimize_merge_order(transcriptions: List[str]):
    """
    Optional function to optimize the order of merging based on overlap size.
    This can be helpful for very large sets of transcriptions.
    
    Returns a reordered list of transcriptions.
    """
    # This is a placeholder for a more complex implementation
    # For now, we assume the transcriptions are already in sequential order
    return transcriptions

def merge_transcriptions_with_preprocess(transcriptions: List[str]):
    """
    Main function that handles preprocessing and merging of multiple transcriptions.
    
    Args:
        transcriptions: List of text transcriptions
    
    Returns:
        A single merged transcription
    """
    if not transcriptions:
        return ""
    
    # Remove empty transcriptions
    transcriptions = [t for t in transcriptions if t.strip()]
    
    if not transcriptions:
        return ""
    
    if len(transcriptions) == 1:
        return transcriptions[0]
    
    # Optionally optimize the order of merging
    optimized_transcriptions = optimize_merge_order(transcriptions)
    
    # Perform the merging
    return merge_multiple_transcriptions(optimized_transcriptions)

