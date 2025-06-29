from openai import OpenAI
import config
import aiohttp
import requests
import time
import sys
import json
import random
import importlib.util  # For dynamic importing to avoid circular imports

client = OpenAI(api_key=config.OPENAI_API_KEY)

# Try to import labeling module for sentiment label application
try:
    import labeling
    LABELING_AVAILABLE = True
    print("Labeling module successfully imported", file=sys.stderr)
except ImportError:
    LABELING_AVAILABLE = False
    print("Labeling module not available, sentiment labels will not be applied", file=sys.stderr)

# In-memory cache for sentiment distribution to avoid repeated API calls
sentiment_cache = {}
sentiment_cache_expiry = {}


def takeFirstSentence(paragraph):
  firstSentence = paragraph.split(".")[0]
  return firstSentence


def get_last_number(s):
  """
  This function checks if the last character of the string is a number,
  ignoring any trailing whitespace. If it is, the function returns that number.
  Otherwise, it returns None.

  :param s: The input string
  :return: The last number if it exists, otherwise None
  """
  s = s.rstrip()  # Remove trailing whitespace
  if s and s[-1].isdigit():
    return int(s[-1])
  return None


async def conversation_format(conversation, sender_id=None, page_id=None, conversation_id=None):
  """
  Format conversation for sentiment analysis and persist results to database
  
  :param conversation: The conversation data
  :param sender_id: The sender ID (optional)
  :param page_id: The page ID (optional)
  :param conversation_id: The conversation ID (optional)
  :return: Sentiment analysis result (category, rank)
  """
  dialogue = "\n".join([
      f'User: {entry["user"]}\nBot: {entry["bot"]}' for entry in conversation
  ])
  print(dialogue)
  return await sentiment_analysis(
      transcription=dialogue,
      sender_id=sender_id,
      page_id=page_id,
      conversation_id=conversation_id
  )


#SenderPsid,
async def save_sentiment_to_db(page_id, sender_id, conversation_id, message_text, sentiment_rank, sentiment_category):
  """
  Save sentiment analysis results to PostgreSQL database for persistence
  and apply appropriate Facebook label based on sentiment rank
  
  :param page_id: The page ID
  :param sender_id: The sender/user ID
  :param conversation_id: The conversation ID if available
  :param message_text: The text that was analyzed (limited to 100 chars)
  :param sentiment_rank: The sentiment rank (1-5)
  :param sentiment_category: The sentiment category (negative, neutral, positive)
  """
  try:
    import datetime
    import os
    import importlib
    from db_helper import get_db_connection, return_db_connection
    
    # Get current date in YYYY-MM-DD format
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # Connect to PostgreSQL database using connection pool
    conn = get_db_connection()
    if not conn:
      print("Failed to get database connection", file=sys.stderr)
      return False
      
    cursor = conn.cursor()
    
    # Format the sentiment rank label similar to what we do in JS
    sentiment_label = f"Rank {sentiment_rank} / 5.0"
    
    # Check if we already have an entry for this sender_id and page_id
    cursor.execute("""
      SELECT id FROM sentiment_distribution 
      WHERE page_id = %s AND sender_id = %s
    """, (page_id, sender_id))
    
    existing_row = cursor.fetchone()
    
    if existing_row:
      # Update existing record
      cursor.execute("""
        UPDATE sentiment_distribution 
        SET rank = %s, label = %s, updated_at = NOW()
        WHERE id = %s
      """, (float(sentiment_rank), sentiment_label, existing_row[0]))
      print(f"Updated sentiment_distribution record for page_id={page_id}, sender_id={sender_id}", file=sys.stderr)
    else:
      # Insert new record
      cursor.execute("""
        INSERT INTO sentiment_distribution 
        (page_id, sender_id, rank, label, created_at, updated_at) 
        VALUES (%s, %s, %s, %s, NOW(), NOW())
      """, (page_id, sender_id, float(sentiment_rank), sentiment_label))
      print(f"Inserted new sentiment_distribution record for page_id={page_id}, sender_id={sender_id}", file=sys.stderr)
    
    # Update user_states table with sentiment rank if it exists
    cursor.execute("""
      SELECT id FROM user_states
      WHERE page_id = %s AND sender_id = %s
    """, (page_id, sender_id))
    
    user_state_row = cursor.fetchone()
    
    if user_state_row:
      cursor.execute("""
        UPDATE user_states 
        SET rank = %s, updated_at = NOW()
        WHERE id = %s
      """, (float(sentiment_rank), user_state_row[0]))
      print(f"Updated user_states record with sentiment rank for page_id={page_id}, sender_id={sender_id}", file=sys.stderr)
    
    # Commit database changes
    conn.commit()
    cursor.close()
    return_db_connection(conn)
    
    print(f"Saved sentiment data to PostgreSQL database: page_id={page_id}, sender_id={sender_id}, rank={sentiment_rank}", file=sys.stderr)
    
    # INTEGRATION WITH FACEBOOK LABELING SYSTEM
    # Only apply labels to Facebook Messenger users (not for Instagram)
    try:
      # Import the labeling module dynamically to avoid circular imports
      labeling_spec = importlib.util.find_spec('labeling')
      if labeling_spec:
        labeling = importlib.util.module_from_spec(labeling_spec)
        labeling_spec.loader.exec_module(labeling)
        
        # First, check if we have the necessary sentiment labels already created
        label_name = f"Sentiment: {sentiment_label}"
        print(f"Looking for sentiment label: {label_name}", file=sys.stderr)
        
        # Get the label ID for this sentiment rank
        label_id = await labeling.get_label_id(label_name, page_id)
        
        # If label doesn't exist, create it
        if label_id == "No label found":
          print(f"Creating new sentiment label: {label_name}", file=sys.stderr)
          label_id = await labeling.add_custom_label(label_name, page_id)
          print(f"Created new label with ID: {label_id}", file=sys.stderr)
        
        # Only proceed if we have a valid label ID
        if label_id and label_id != "No label found":
          print(f"Applying sentiment label {label_id} to user {sender_id}", file=sys.stderr)
          
          # Apply the label to the user
          result = await labeling.Associate_Label_to_User(sender_id, label_id, page_id)
          print(f"Label application result: {result}", file=sys.stderr)
        else:
          print(f"No valid label ID found for {label_name}", file=sys.stderr)
      else:
        print(f"Labeling module not found", file=sys.stderr)
    except Exception as label_error:
      print(f"Error applying Facebook sentiment label: {str(label_error)}", file=sys.stderr)
      # Continue execution even if labeling fails
    
    return True
    
  except Exception as db_error:
    print(f"Error saving sentiment to PostgreSQL database: {str(db_error)}", file=sys.stderr)
    if 'conn' in locals() and conn:
      conn.rollback()
      return_db_connection(conn)
    return False

async def sentiment_analysis(transcription, page_id=None, sender_id=None, conversation_id=None):
  """
  Optimized sentiment analysis function that:
  1. Limits input to 100 characters (first part of message only)
  2. Uses a simple prompt for faster responses
  3. Has better error handling and parsing
  4. Uses a structured response format
  5. Has built-in timeout handling
  6. Now saves results to database for persistence
  """
  # Limit transcription length to 100 chars to improve response time
  if transcription and len(transcription) > 100:
    transcription = transcription[:100]
    
  try:
    # Optimized prompt for faster processing
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=5,  # Reduced from 50 to 5 - we only need a single digit
        temperature=0.2,  # Reduced variance for more consistent results
        response_format={"type": "text"},  # Ensure simple text response
        messages=[{
            "role": "system",
            "content": "You will analyze message sentiment and classify on a scale from 1-5 where: 1=very negative, 2=negative, 3=neutral, 4=positive, 5=very positive. Respond with ONLY a single digit 1-5."
        }, {
            "role": "user",
            "content": transcription
        }]
    )

    # Get raw content and find the first digit
    raw_content = response.choices[0].message.content.strip()
    print(f"Sentiment raw response: {raw_content}", file=sys.stderr)
    
    # First, try to parse the entire string as an integer
    try:
      sentiment = int(raw_content)
      # Ensure the value is within our range
      if sentiment < 1 or sentiment > 5:
        sentiment = 3  # Default to neutral for out-of-range values
    except ValueError:
      # If that fails, extract the first digit from the response
      digits = [char for char in raw_content if char.isdigit()]
      if digits and 1 <= int(digits[0]) <= 5:
        sentiment = int(digits[0])
      else:
        sentiment = 3  # Default to neutral if no valid digit found
    
    # Map sentiment value to category
    if sentiment <= 2:
      category = "negative"
    elif sentiment == 3:
      category = "neutral"
    else:
      category = "positive"
      
    print(f"Final sentiment: {category} ({sentiment})", file=sys.stderr)
    
    # Save to database if we have the necessary identifiers
    if page_id and sender_id:
      await save_sentiment_to_db(
        page_id=page_id,
        sender_id=sender_id,
        conversation_id=conversation_id,
        message_text=transcription,
        sentiment_rank=sentiment,
        sentiment_category=category
      )
    
    return (category, sentiment)
    
  except Exception as e:
    print(f"Error in sentiment analysis: {str(e)}", file=sys.stderr)
    return ("neutral", 3)  # Default to neutral in case of errors
    
def get_sentiment_distribution(page_id, days=7):
    """
    Get the sentiment distribution for a page over the specified number of days.
    Fetches real sentiment data from the database or conversation history.
    Uses caching to improve performance.
    
    :param page_id: The Facebook page ID
    :param days: Number of days to look back for conversations
    :return: List of sentiment ranks and counts
    """
    import sys
    global sentiment_cache, sentiment_cache_expiry
    
    import datetime
    import os
    from config import get_access_token
    
    # Check cache first - use cache if it exists and is less than 15 minutes old (reduced from 30 for more frequent updates)
    cache_key = f"{page_id}_{days}"
    current_time = time.time()
    
    if cache_key in sentiment_cache and sentiment_cache_expiry.get(cache_key, 0) > current_time:
        print(f"Using cached sentiment distribution for page {page_id}", file=sys.stderr)
        return sentiment_cache[cache_key]
    
    # Map Instagram page ID to Facebook page ID if needed
    original_page_id = page_id
    if page_id == '17841456783426236':  # Instagram page ID
        page_id = '420350114484751'  # Mapped Facebook page ID
        print(f"Instagram page ID {original_page_id} detected in insights, mapping to Facebook page ID {page_id}", file=sys.stderr)
    
    print(f"Fetching sentiment distribution for page {page_id} over {days} days", file=sys.stderr)
    
    try:
        # Initialize sentiment counts
        sentiment_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        
        # FIRST PRIORITY: Use live user_state data for real-time sentiment ranks
        # Avoid circular import by using a safer approach to access user_state
        try:
            import sys
            import importlib.util
            
            # Use importlib to safely import assistant_handler module
            spec = importlib.util.find_spec('assistant_handler')
            if spec:
                # Get the module namespace without executing the code again
                module = sys.modules.get('assistant_handler')
                if module and hasattr(module, 'user_state'):
                    user_state = module.user_state
                    rank_found = False
                    
                    # Count ranks from all users for this page
                    for user_id, state in user_state.items():
                        if state.get("page_id") == page_id and "rank" in state:
                            rank = state["rank"]
                            if 1 <= rank <= 5:
                                sentiment_counts[rank] += 1
                                rank_found = True
                    
                    # If we found ranks in user_state, use that data
                    if rank_found:
                        # Format the distribution for the frontend
                        sentiment_distribution = [
                            {'rank': rank, 'count': count} for rank, count in sentiment_counts.items()
                        ]
                        
                        # Cache the result for 5 minutes (short time for real-time data)
                        sentiment_cache[cache_key] = sentiment_distribution
                        sentiment_cache_expiry[cache_key] = current_time + (5 * 60)  # 5 minutes
                        
                        print(f"Using real-time user_state sentiment ranks: {sentiment_distribution}", file=sys.stderr)
                        return sentiment_distribution
        except Exception as state_error:
            print(f"Error safely accessing user_state for ranks: {str(state_error)}", file=sys.stderr)
        
        # SECOND PRIORITY: Check if we have PostgreSQL database for sentiment data
        try:
            from db_helper import get_db_connection, return_db_connection
            
            # Connect to PostgreSQL
            conn = get_db_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    
                    # Calculate date range
                    end_date = datetime.datetime.now()
                    start_date = end_date - datetime.timedelta(days=days)
                    start_date_str = start_date.strftime('%Y-%m-%d')
                    
                    # Try to get data from sentiment_distribution table first
                    cursor.execute("""
                        SELECT CAST(rank AS INTEGER), COUNT(*) as count 
                        FROM sentiment_distribution 
                        WHERE page_id = %s AND created_at >= %s
                        GROUP BY CAST(rank AS INTEGER)
                        ORDER BY CAST(rank AS INTEGER)
                    """, (page_id, start_date_str))
                    
                    rows = cursor.fetchall()
                    
                    if rows:
                        print(f"Found {len(rows)} sentiment ranks in PostgreSQL sentiment_distribution table", file=sys.stderr)
                        # Update the counts with data from database
                        for rank, count in rows:
                            rank_int = int(rank)
                            if 1 <= rank_int <= 5:
                                sentiment_counts[rank_int] = count
                    else:
                        # If no data in sentiment_distribution, try user_states table for rank data
                        cursor.execute("""
                            SELECT CAST(rank AS INTEGER), COUNT(*) as count 
                            FROM user_states 
                            WHERE page_id = %s AND created_at >= %s AND rank IS NOT NULL
                            GROUP BY CAST(rank AS INTEGER)
                            ORDER BY CAST(rank AS INTEGER)
                        """, (page_id, start_date_str))
                        rows = cursor.fetchall()
                        
                        if rows:
                            print(f"Found {len(rows)} sentiment ranks in user_states table", file=sys.stderr)
                            # Update the counts with data from database
                            for rank, count in rows:
                                rank_int = int(rank)
                                if 1 <= rank_int <= 5:
                                    sentiment_counts[rank_int] = count
                    
                    # Close the database cursor
                    cursor.close()
                    
                    # If we found data in the database and it's not all zeros, use it
                    if sum(sentiment_counts.values()) > 0:
                        # Format the distribution for the frontend
                        sentiment_distribution = [
                            {'rank': rank, 'count': count} for rank, count in sentiment_counts.items()
                        ]
                        
                        # Cache the result for 15 minutes (reduced from 30 for more frequent updates)
                        sentiment_cache[cache_key] = sentiment_distribution
                        sentiment_cache_expiry[cache_key] = current_time + (15 * 60)  # 15 minutes
                        
                        print(f"Using database sentiment distribution: {sentiment_distribution}", file=sys.stderr)
                        return sentiment_distribution
                finally:
                    # Return the connection to the pool
                    return_db_connection(conn)
        except Exception as db_error:
            print(f"Error accessing database: {str(db_error)}", file=sys.stderr)
        
        # If database had no data, try access token
        access_token = get_access_token(page_id)
        
        if not access_token:
            print(f"No access token found for page {page_id}", file=sys.stderr)
            # Return empty sentiment distribution with real zeros - no synthetic data
            result = [
                {'rank': 1, 'count': 0},
                {'rank': 2, 'count': 0},
                {'rank': 3, 'count': 0},
                {'rank': 4, 'count': 0},
                {'rank': 5, 'count': 0}
            ]
            
            # Cache the result for 15 minutes
            sentiment_cache[cache_key] = result
            sentiment_cache_expiry[cache_key] = current_time + (15 * 60)  # 15 minutes
            
            print("No access token, using real zero values for sentiment distribution", file=sys.stderr)
            return result
        
        # Try to get conversation data from Facebook API (with timeout and limits)
        print(f"Fetching conversations from Facebook API for page {page_id}", file=sys.stderr)
        try:
            # Use the optimized approach with minimal requested data
            conversations_response = requests.get(
                f"https://graph.facebook.com/v18.0/{page_id}/conversations",
                params={
                    'access_token': access_token,
                    'fields': 'messages.limit(1){from,message}',  # Only retrieve one message per conversation with minimal fields
                    'limit': 3  # Only get 3 conversations total - significant performance improvement
                },
                timeout=3  # Reduced timeout for faster failure
            )
            
            conversations = []
            if conversations_response.status_code == 200:
                conversations_data = conversations_response.json()
                conversations = conversations_data.get('data', [])
                print(f"Retrieved {len(conversations)} conversations from Facebook", file=sys.stderr)
            else:
                print(f"Failed to get conversations for page {page_id}: {conversations_response.status_code}", file=sys.stderr)
            
            # Prepare for batch processing - collect all message texts
            message_texts = []
            
            # Extract the first message from each conversation (already limited to 1 in the API query)
            for idx, conversation in enumerate(conversations[:3]):  # Ensure max 3 conversations
                try:
                    messages = conversation.get('messages', {}).get('data', [])
                    if messages and len(messages) > 0 and 'message' in messages[0]:
                        # Only take first 100 chars of the message
                        message_text = messages[0]['message'][:100].strip()
                        if message_text:
                            message_texts.append(message_text)
                            print(f"Extracted message: {message_text[:30]}...", file=sys.stderr)
                except Exception as extract_error:
                    print(f"Error extracting message {idx+1}: {str(extract_error)}", file=sys.stderr)
            
            # Initialize sentiment counts for all ranks
            sentiment_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
            
            # Process messages - use ultra efficient API calls
            for idx, message_text in enumerate(message_texts):
                try:
                    # Simplified prompt with no backstory - just direct instructions
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        max_tokens=1,  # We only need a single digit
                        temperature=0,  # Most consistent results
                        response_format={"type": "text"},  # Ensure simple text response
                        messages=[{
                            "role": "system",
                            "content": "Rate sentiment 1-5 (1=negative, 3=neutral, 5=positive). Reply with ONLY a single digit."
                        }, {
                            "role": "user",
                            "content": message_text
                        }]
                    )
                    
                    # Direct digit extraction - most efficient
                    result = response.choices[0].message.content.strip()
                    if result.isdigit() and 1 <= int(result) <= 5:
                        sentiment_rank = int(result)
                        sentiment_counts[sentiment_rank] += 1
                        print(f"Message {idx+1}: Rank {sentiment_rank}", file=sys.stderr)
                    else:
                        # Default to neutral for non-digit responses
                        sentiment_counts[3] += 1
                        print(f"Message {idx+1}: Invalid response '{result}', using neutral (3)", file=sys.stderr)
                except Exception as api_error:
                    print(f"Error in sentiment API call for message {idx+1}: {str(api_error)}", file=sys.stderr)
                    # Default to neutral on errors
                    sentiment_counts[3] += 1
            
            # Ensure we have values for all ranks
            if sum(sentiment_counts.values()) > 0:
                # Add small values for any empty ranks
                for rank in range(1, 6):
                    if sentiment_counts[rank] == 0:
                        sentiment_counts[rank] = 1
            else:
                # Use all zeros - no synthetic data
                sentiment_counts = {
                    1: 0,
                    2: 0, 
                    3: 0,
                    4: 0,
                    5: 0
                }
                print("No sentiment data from API, using real zeros", file=sys.stderr)
        except Exception as e:
            print(f"Error getting Facebook data: {str(e)}", file=sys.stderr)
            # Use real zero values - no synthetic data
            sentiment_counts = {
                1: 0,
                2: 0,
                3: 0,
                4: 0,
                5: 0
            }
            print("Error in Facebook API, using real zero values", file=sys.stderr)
        
        # Format the distribution for the frontend
        sentiment_distribution = [
            {'rank': rank, 'count': count} for rank, count in sentiment_counts.items()
        ]
        
        # Cache the results for 30 minutes
        sentiment_cache[cache_key] = sentiment_distribution
        sentiment_cache_expiry[cache_key] = current_time + (30 * 60)  # 30 minutes
        
        print(f"Final sentiment distribution: {sentiment_distribution}", file=sys.stderr)
        return sentiment_distribution
    except Exception as e:
        print(f"Error in sentiment distribution: {str(e)}", file=sys.stderr)
        # Return real zeros - no synthetic data
        result = [
            {'rank': 1, 'count': 0},
            {'rank': 2, 'count': 0},
            {'rank': 3, 'count': 0},
            {'rank': 4, 'count': 0},
            {'rank': 5, 'count': 0}
        ]
        
        # Cache the result for 15 minutes
        if 'cache_key' in locals() and 'sentiment_cache' in globals():
            sentiment_cache[cache_key] = result
            sentiment_cache_expiry[cache_key] = current_time + (15 * 60)  # 15 minutes
        
        print("Error in sentiment analysis, using real zero values", file=sys.stderr)
        return result



###### convert sentiment with other bot one take analysis and one make format with two tuple rank number and explanation of rank 
 #Always start your response with the assigned rating, followed by a brief explanation justifying the rating.

      # Sentiment_number = 5.0
      # Sentiment = takeFirstSentence(Sentiment)
      # print(Sentiment)
      # Sentiment_Word = "neutral"
      # if "positive" in Sentiment:
      #   Sentiment_number = 4.0
      #   Sentiment_Word = "positive"
      #   print(Sentiment_Word)
      #   return (Sentiment_Word, Sentiment_number)
      # elif "negative" in Sentiment:
      #   Sentiment_number = 1.0
      #   Sentiment_Word = "negative"
      #   print(Sentiment_Word)
      #   return (Sentiment_Word, Sentiment_number)
      # else:
      #   print(Sentiment_Word)
      #   return (Sentiment_Word, Sentiment_number)
  # else:
  #   return "negative", 0.0

#As an AI with expertise in language and emotion analysis, your task is to analyze the sentiment of the following text. Please consider the overall tone of the discussion, the emotion conveyed by the language used, and the context in which words and phrases are used.Use a scale from 1 to 10, where 0 to 4 means negative, 5 means neutral, and 6 to 10 means positive. Indicate whether the sentiment is generally positive, negative, or neutral, and provide brief explanations for your analysis where possible.

#As an AI with expertise in language and emotion analysis, your task is to analyze the sentiment of the following text. Please consider the overall tone of the discussion, the emotion conveyed by the language used, and the context in which words and phrases are used. rate the conversation on scale from 1 to 10 . Indicate whether the sentiment is generally positive, negative, or neutral , put your output in the format of a number from 1 to 10. For example, if the conversation is generally positive, you can return a value of 10. If the conversation is generally negative, you can return a value of 1. If the conversation is neutral, you can return a value of 5.

# f'''As an AI with expertise in language and emotion analysis, your task is to analyze the sentiment of the following dialouge between user and bot. Analyze whole conversation and the analysis will evaluate according user words as follow:
# - Conversations that include cursing, absolute attacks, and all forms of insults against the Christian faith, attempts to blackmail people by asking for money, talking about sex, or talking about anything other than Christianity and Islam are considered negative conversations and receive a rating of 1 or 2 out of 5.
# - Conversations that include questions about the Christian faith, such as who is Christ? After answering, there is an argument and more than one question is opened. This is considered a normal conversation and receives a rating of 3 out of 5.
# - Conversations in which a person says, “I searched Christianity,” or “I want the Bible,” or asks a real question about the Christian faith, receive a rating of 4 or 5 out of 5.
# - Your output response should be a numeral between 1 and 5, where 1 and 2 represents a range of negative sentiment and 4 and 5 represents a range of positive sentiment. And 3 represents neutral sentiment.'''
