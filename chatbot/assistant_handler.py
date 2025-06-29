import asyncio
import json
import random
import re
import sys
import threading
import time
from datetime import datetime, timedelta
from types import new_class

import aiohttp
import requests
from flask import Flask, jsonify, render_template, request
from flask.wrappers import Response
from openai.types.beta import assistant

import config
import handeling_User
import handle_message
import labeling
import sentiment


# Import from db_persistence instead of defining locally to avoid circular imports
from db_persistence import save_user_state_to_db

# This function is just a wrapper that redirects to the imported function
# to maintain backward compatibility
async def save_user_state_to_db_legacy(sender_id, state):
    """
    Legacy wrapper that redirects to db_persistence.save_user_state_to_db
    
    :param sender_id: The sender ID
    :param state: The user state dictionary
    :return: Success status
    """
    print(f"[DEPRECATED] Using legacy save_user_state_to_db wrapper, please update imports", file=sys.stderr)
    return await save_user_state_to_db(sender_id, state)


async def callSendAPI(senderPsid, response, page_id):
  """
  Send API call using Node.js message sending endpoint
  This ensures proper routing for Instagram accounts linked to Facebook pages
  """
  # Convert TextContentBlock to dict if necessary
  if hasattr(response, '__dict__'):
    response = {'text': str(response)}
  # Ensure response is properly formatted
  if isinstance(response, dict) and 'text' in response:
    message_text = response['text']
  else:
    message_text = str(response)

  # Use Node.js endpoint which has correct Instagram routing logic
  payload = {
      'recipientId': senderPsid,
      'pageId': page_id,
      'text': message_text
  }
  
  headers = {'content-type': 'application/json'}
  
  # Call Node.js message sending endpoint
  node_url = 'http://localhost:5000/api/send-facebook-message'
  
  try:
    async with aiohttp.ClientSession() as session:
      async with session.post(node_url, json=payload, headers=headers, timeout=10) as r:
        response_text = await r.text()
        if r.status == 200:
          print(f"Message sent successfully via Node.js endpoint: {response_text}", file=sys.stderr)
        else:
          print(f"Error sending message via Node.js endpoint: {r.status} - {response_text}", file=sys.stderr)
  except Exception as e:
    print(f"Error calling Node.js message endpoint: {str(e)}", file=sys.stderr)
    # Fallback error handling
    raise e


"""
Check if a conversation contains the greeting message sent by the bot/page.

This is a critical function for distinguishing between different types of conversations:
1. Conversations initiated through Facebook Ads will contain the greeting message sent BY THE PAGE
2. Regular conversations started by users will not contain the greeting message from the page

CRITICAL FIX (2024-May-5):
Previously, this function was incorrectly checking ALL messages for the greeting text,
which could lead to false positives if a user happened to include the greeting text in their message.
Now it ONLY checks messages FROM THE PAGE/BOT for the greeting text, not messages from users.

Args:
    senderPSID (str): The PSID of the sender (user)
    page_id (str): The ID of the Facebook page

Returns:
    bool: True if the conversation contains the greeting message sent by the page, False otherwise
"""
async def check_greeting_message(senderPSID, page_id):
  """
  Check if the conversation contains the greeting message.
  
  Logic:
  - If greeting message is empty (""), bot should respond to all users
  - If greeting exists, check if any of the last 4 bot messages contains it
  - If greeting is found in bot messages, bot should respond
  - If not found, bot should NOT respond (handled by follow-up team)
  
  Returns:
    bool: False if bot should respond, True if bot should NOT respond
  """
  # Use dynamic_config to ensure we get the most up-to-date greeting message
  from dynamic_config import get_dynamic_greeting_message
  greeting = get_dynamic_greeting_message(page_id)
  
  print(f"[check_greeting] Checking greeting message for page {page_id}: '{greeting}'")
  
  # Case 1: If greeting message is empty, bot should respond to ALL users
  if not greeting or greeting.strip() == "":
    print(f"[check_greeting] Empty greeting message for page {page_id}, bot should respond to all users")
    return False  # Return False to activate bot
  
  # Case 2: Greeting message exists, check for it in conversation
  # Get conversation_id
  conversation_id = await handle_message.get_conversation_id_for_user(senderPSID, page_id)
  
  # If no conversation exists, this is a new user and bot should respond
  if not conversation_id:
    print(f"[check_greeting] No conversation found for user {senderPSID}, treating as new conversation")
    print(f"[check_greeting] For new conversations with greeting message '{greeting}', bot should respond")
    return False  # Return False to activate bot (new conversation)
  
  # Get messages for this conversation
  all_messages = await handle_message.get_messages_for_conversation(conversation_id, page_id)
  print(f"[check_greeting] Found {len(all_messages)} total messages in conversation")
  
  # Get only bot messages (from page)
  bot_messages = [msg for msg in all_messages if msg.get('from', {}).get('id') == page_id]
  print(f"[check_greeting] Found {len(bot_messages)} bot messages in conversation")
  
  # Get last 4 bot messages (or all if less than 4)
  last_bot_messages = bot_messages[-4:] if len(bot_messages) > 4 else bot_messages
  
  # Check if any of the last 4 bot messages contains the greeting
  for idx, message in enumerate(last_bot_messages):
    message_text = message.get('message', '')
    print(f"[check_greeting] Checking bot message {idx+1}: '{message_text}'")
    
    # Check if greeting is a substring of the message
    if greeting in message_text:
      print(f"[check_greeting] Found greeting '{greeting}' in message: '{message_text}'")
      return False  # Return False to activate bot
    
    # Additional check with Unicode normalization for Arabic text
    import unicodedata
    normalized_greeting = unicodedata.normalize('NFC', greeting)
    normalized_message = unicodedata.normalize('NFC', message_text)
    if normalized_greeting in normalized_message:
      print(f"[check_greeting] Found normalized greeting in message after Unicode normalization")
      return False  # Return False to activate bot
  
  # If we reach here, greeting was not found in any bot message
  print(f"[check_greeting] No message containing greeting '{greeting}' found in last {len(last_bot_messages)} bot messages")
  return True  # Return True to deactivate bot (greeting not found)


#this a user state for track and personlize the user
user_state = {}
# Add this tracking set at module level
processed_message_ids = set()


async def get_assistant_response(senderPSID, Recieved_Message, page_id):
    # Ensure we have a string page_id, not a config object
    if not isinstance(page_id, str):
        print(f"WARNING: page_id is not a string but {type(page_id)}")
        page_id = str(page_id)
        
    # Special case for Instagram
    if page_id == '17841456783426236':
        print(f"Instagram page ID detected, mapping to Facebook page ID 420350114484751")
        page_id = '420350114484751'  # Use the Rodood Facebook page ID
        
    print("get assistant response now!")
    # Check for duplicate message processing
    message_id = Recieved_Message.get('mid', None)
    if message_id:
        if message_id in processed_message_ids:
            print(f"Message {message_id} already processed, skipping")
            return "DUPLICATE_MESSAGE", 200
        else:
            # Mark this message as being processed
            processed_message_ids.add(message_id)
            # Limit the size of the set to prevent memory issues
            if len(processed_message_ids) > 1000:
                # Remove oldest entries (approximately)
                try:
                    for _ in range(100):
                        processed_message_ids.pop()
                except KeyError:
                    pass

    if 'text' in Recieved_Message:
        user_message = Recieved_Message['text']
        print("User Message:", user_message)
        # First, check if user already exists in PostgreSQL database
        try:
            from db_helper import get_db_connection, return_db_connection
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                
                # Check if user exists in user_states table
                cursor.execute("""
                    SELECT id, message_count, labels, conversation_id, thread_id, run_id, 
                           is_new_user, has_stop_message, last_message, rank, messages_context, conversation
                    FROM user_states 
                    WHERE sender_id = %s AND page_id = %s
                """, (senderPSID, page_id))
                
                user_row = cursor.fetchone()
                
                if user_row:
                    # User exists in database, load their state
                    print(f"Found existing user {senderPSID} for page {page_id} in PostgreSQL database", file=sys.stderr)
                    
                    # Convert database row to user state
                    # CRITICAL FIX: Parse JSON fields correctly from the database
                    try:
                        labels_from_db = json.loads(user_row[2]) if user_row[2] else []
                        messages_context_from_db = json.loads(user_row[10]) if user_row[10] else []
                        conversation_from_db = json.loads(user_row[11]) if user_row[11] else []
                        print(f"[DEBUG] Successfully parsed JSON fields from database", file=sys.stderr)
                    except Exception as e:
                        print(f"[DEBUG] Error parsing JSON fields from database: {e}", file=sys.stderr)
                        labels_from_db = []
                        messages_context_from_db = []
                        conversation_from_db = []
                    
                    user_state[senderPSID] = {
                        "page_id": page_id,
                        "message_count": user_row[1],
                        "label": labels_from_db,  # CRITICAL FIX: Field name mismatch fixed
                        "conversation_id": user_row[3],
                        "thread_id": user_row[4],
                        "run_id": user_row[5],
                        "new_user": user_row[6],  # Database column is is_new_user
                        "has_stop_message": user_row[7],
                        "last_message": user_row[8],
                        "rank": user_row[9],  # Use lowercase to match database schema
                        "messages_context": messages_context_from_db,
                        "conversation": conversation_from_db
                    }
                    
                    print(f"Loaded user state from database: message_count={user_row[1]}, labels={labels_from_db}", file=sys.stderr)
                else:
                    # First time seeing this user, create new entry
                    print(f"New user {senderPSID} for page {page_id}, creating fresh state", file=sys.stderr)
                    greeting_response = await check_greeting_message(senderPSID, page_id)
                    
                    # Initialize user state
                    # Start a new array for context_messages
                    user_state[senderPSID] = {
                        "page_id": page_id,
                        "message_count": 1,
                        "label": [],
                        "conversation": [],
                        "conversation_id": None,
                        "new_user": True,
                        "thread_id": None,
                        "run_id": None,
                        "messages_context": [],
                        "last_message_time": None,
                        "has_stop_message": False,
                        "last_message": user_message,
                        "rank": None  # Use lowercase to match database schema
                    }
                
                # Return connection and cursor
                cursor.close()
                return_db_connection(conn)
            else:
                print("Failed to get database connection, falling back to in-memory state", file=sys.stderr)
        except Exception as db_error:
            print(f"Error accessing database: {str(db_error)}", file=sys.stderr)
            # Print traceback for better debugging
            import traceback
            print(traceback.format_exc(), file=sys.stderr)
        
        # If user not in state yet (i.e., error in database access), initialize
        if senderPSID not in user_state:
            greeting_response = False  # Default if we can't check with database
            # Fallback initialization if database access failed
            user_state[senderPSID] = {
                "page_id": page_id,
                "message_count": 1,
                "label": [],
                "conversation": [],
                "conversation_id": None,
                "new_user": True,
                "thread_id": None,
                "run_id": None,
                "messages_context": [],
                "last_message_time": None,
                "has_stop_message": False,
                "last_message": user_message,
                "rank": None  # Use lowercase to match database schema
            }
            
        # Update message count
        user_state[senderPSID]["message_count"] += 1
        user_state[senderPSID]["last_message"] = user_message
        
        # Reset stop message flag if user sends a new message
        if user_state[senderPSID]["has_stop_message"]:
            print(f"Clearing stop message flag for user {senderPSID}", file=sys.stderr)
            user_state[senderPSID]["has_stop_message"] = False
        
        # Check for end of conversation based on message count
        max_messages = config.get_max_messages(page_id)
        if user_state[senderPSID]["message_count"] > max_messages:
            # Send end message
            end_message = config.get_end_message(page_id)
            
            if not end_message:  # Fallback if config doesn't have an end message
                end_message = "Thank you for chatting with us today. We've reached the end of our conversation."
                
            await callSendAPI(senderPSID, {"text": end_message}, page_id)
            
            # Reset state for a potential new conversation
            user_state[senderPSID]["message_count"] = 0
            user_state[senderPSID]["has_stop_message"] = True
            
            # Save the updated state to the database
            await save_user_state_to_db(senderPSID, user_state[senderPSID])
            
            return "END_OF_CONVERSATION", 200
        
        # Process user's message using OpenAI
        response = await handeling_User.get_chatgpt_response(user_message, user_state[senderPSID], senderPSID, page_id)
        
        # Update messages context in user state before saving
        # This ensures the conversation history is maintained
        if not user_state[senderPSID]["messages_context"]:
            user_state[senderPSID]["messages_context"] = []
            
        # Add the user message to the context
        user_state[senderPSID]["messages_context"].append({
            "role": "user",
            "content": user_message
        })
        
        # Add the assistant's response to the context
        user_state[senderPSID]["messages_context"].append({
            "role": "assistant",
            "content": response
        })
        
        # Update the timestamp for the last message
        user_state[senderPSID]["last_message_time"] = int(time.time())
        
        # Perform sentiment analysis on the user message before saving
        try:
            print(f"*** SENTIMENT ANALYSIS: Starting analysis for user {senderPSID} on page {page_id} ***", file=sys.stderr)
            print(f"*** SENTIMENT ANALYSIS: Message to analyze: '{user_message}' ***", file=sys.stderr)
            
            # Call sentiment analysis with both message content and identifiers
            sentiment_result = await sentiment.sentiment_analysis(
                user_message,
                page_id=page_id,
                sender_id=senderPSID,
                conversation_id=user_state[senderPSID].get("conversation_id")
            )
            
            # Update user state with sentiment rank
            if sentiment_result and isinstance(sentiment_result, tuple) and len(sentiment_result) >= 2:
                category, rank = sentiment_result
                print(f"*** SENTIMENT ANALYSIS SUCCESS: Result={category} (rank: {rank}) ***", file=sys.stderr)
                # Use lowercase "rank" to match database schema
                user_state[senderPSID]["rank"] = rank
            else:
                print(f"*** SENTIMENT ANALYSIS: Invalid result format: {sentiment_result} ***", file=sys.stderr)
        except Exception as sentiment_error:
            print(f"Error performing sentiment analysis: {str(sentiment_error)}", file=sys.stderr)
            # Continue execution even if sentiment analysis fails
        
        # Save updated user state to the database
        await save_user_state_to_db(senderPSID, user_state[senderPSID])
        
        # Return the response from the assistant
        return response
        
    else:
        # If there's no text in the message, check for attachments
        if 'attachments' in Recieved_Message:
            attachments = Recieved_Message['attachments']
            # Send a default acknowledgment of the attachment
            response = "I've received your attachment. If you have any specific questions about it, please let me know."
            await callSendAPI(senderPSID, {"text": response}, page_id)
            return response
        else:
            # Default fallback response
            response = "I didn't understand your message. Please try sending a text message."
            await callSendAPI(senderPSID, {"text": response}, page_id)
            return response


async def check_sentiment_every_24_hours():
    # Get all user states from the database
    try:
        from db_helper import get_db_connection, return_db_connection
        
        while True:
            print("Running sentiment check job...", file=sys.stderr)
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                
                # Get all user states that haven't been checked for sentiment in 24 hours
                # This assumes we have a last_sentiment_check field in the database
                current_time = int(time.time())
                twenty_four_hours_ago = current_time - (24 * 60 * 60)
                
                cursor.execute("""
                    SELECT sender_id, page_id, messages_context
                    FROM user_states
                    WHERE last_sentiment_check IS NULL 
                       OR last_sentiment_check < %s
                """, (twenty_four_hours_ago,))
                
                users_to_check = cursor.fetchall()
                
                for user_row in users_to_check:
                    sender_id = user_row[0]
                    page_id = user_row[1]
                    
                    try:
                        messages_context = json.loads(user_row[2]) if user_row[2] else []
                        
                        # Only analyze sentiment if we have enough messages
                        if len(messages_context) >= 5:
                            # Get all user messages from the conversation
                            user_messages = [msg["content"] for msg in messages_context if msg["role"] == "user"]
                            
                            # Concatenate messages for sentiment analysis
                            content_for_analysis = " ".join(user_messages[-5:])  # Use last 5 messages
                            
                            # Perform sentiment analysis directly
                            print(f"Analyzing sentiment for user {sender_id} on page {page_id}", file=sys.stderr)
                            sentiment_result = await sentiment.sentiment_analysis(
                                content_for_analysis,
                                page_id=page_id,
                                sender_id=sender_id,
                                conversation_id=None  # We don't have this in the batch job
                            )
                            
                            if sentiment_result and isinstance(sentiment_result, tuple) and len(sentiment_result) >= 2:
                                category, rank = sentiment_result
                                sentiment_label = f"Sentiment: Rank {rank} / 5.0"
                                
                                # Update user state with sentiment label and rank
                                cursor.execute("""
                                    UPDATE user_states
                                    SET labels = %s, last_sentiment_check = %s, rank = %s
                                    WHERE sender_id = %s AND page_id = %s
                                """, (json.dumps([sentiment_label]), current_time, rank, sender_id, page_id))
                                
                                conn.commit()
                                print(f"Updated sentiment for user {sender_id}: {category} (rank: {rank})", file=sys.stderr)
                    except Exception as e:
                        print(f"Error processing sentiment for user {sender_id}: {str(e)}", file=sys.stderr)
                        continue
                
                cursor.close()
                return_db_connection(conn)
            
            # Wait for 1 hour before next check
            await asyncio.sleep(3600)
    except Exception as e:
        print(f"Error in sentiment check job: {str(e)}", file=sys.stderr)
        # Wait and try again
        await asyncio.sleep(3600)
        # Restart the job
        asyncio.create_task(check_sentiment_every_24_hours())