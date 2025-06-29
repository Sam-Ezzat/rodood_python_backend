"""
Database persistence module for saving user states to the database
This module avoids circular imports by separating persistence logic
"""

import asyncio
import json
import sys
import time
from db_helper import get_db_connection, return_db_connection

async def save_user_state_to_db(sender_id, state):
    """
    Save user state to PostgreSQL database with connection recovery
    
    :param sender_id: The sender ID
    :param state: The user state dictionary
    :return: Success status
    """
    max_retries = 3
    retry_delay = 1  # Start with 1 second delay
    
    for attempt in range(max_retries):
        try:
            # Log the sender_id and page_id for debugging
            page_id = state.get('page_id', 'unknown')
            print(f"DB: Saving state for user {sender_id} on page {page_id} (attempt {attempt + 1})", file=sys.stderr)
            
            # Get a database connection
            conn = get_db_connection()
            if not conn:
                raise Exception("Failed to get database connection")
            
            # Check if the user state already exists
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_states WHERE sender_id = %s", (sender_id,))
            user_exists = cursor.fetchone()[0] > 0
            cursor.close()
            
            # Get current time as ISO-8601 timestamp for PostgreSQL
            # CRITICAL FIX: PostgreSQL expects timestamp, not Unix integer
            from datetime import datetime
            current_time = datetime.now().isoformat()
            
            # Update the timestamp in the state
            state['last_message_time'] = current_time
            
            # Convert lists to JSON strings
            if 'label' in state:
                if isinstance(state['label'], list):
                    # Note: Field in database is 'labels' (plural) but in Python code it's 'label' (singular)
                    state_labels = json.dumps(state['label'])
                else:
                    state_labels = state['label']
            else:
                state_labels = json.dumps([])
                
            if 'conversation' in state:
                if isinstance(state['conversation'], list):
                    state_conversation = json.dumps(state['conversation'])
                else:
                    state_conversation = state['conversation']
            else:
                state_conversation = json.dumps([])
                
            if 'messages_context' in state:
                if isinstance(state['messages_context'], list):
                    state_messages_context = json.dumps(state['messages_context'])
                else:
                    state_messages_context = state['messages_context']
            else:
                state_messages_context = json.dumps([])
                
            # Handle NULL values for optional fields
            thread_id = state.get('thread_id')
            run_id = state.get('run_id')
            conversation_id = state.get('conversation_id')
            
            # Store message count
            message_count = state.get('message_count', 1)
            
            # Handle boolean fields
            # CRITICAL FIX: Field name mismatch - in Python it's "new_user" but in database it's "is_new_user"
            is_new_user = state.get('new_user', True)  # Get from Python's "new_user" but store in DB's "is_new_user"
            has_stop_message = state.get('has_stop_message', False)
            
            # Store the last message
            last_message = state.get('last_message', '')
            
            # Store rank
            rank = state.get('Rank')
                
            if user_exists:
                # Update the existing user state
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE user_states 
                    SET page_id = %s,
                        message_count = %s,
                        labels = %s,
                        conversation = %s,
                        conversation_id = %s,
                        is_new_user = %s,
                        thread_id = %s,
                        run_id = %s,
                        messages_context = %s,
                        last_message_time = %s,
                        has_stop_message = %s,
                        last_message = %s,
                        rank = %s,
                        updated_at = %s
                    WHERE sender_id = %s
                """, (
                    page_id, message_count, state_labels, state_conversation, conversation_id,
                    is_new_user, thread_id, run_id, state_messages_context, current_time,
                    has_stop_message, last_message, rank, current_time, sender_id
                ))
                cursor.close()
                conn.commit()
                print(f"DB: Updated existing user state for {sender_id}", file=sys.stderr)
            else:
                # Insert a new user state
                cursor = conn.cursor()
                # CRITICAL FIX: Use is_new_user column name instead of new_user
                cursor.execute("""
                    INSERT INTO user_states (
                        sender_id, page_id, message_count, labels, conversation, 
                        conversation_id, is_new_user, thread_id, run_id, messages_context,
                        last_message_time, has_stop_message, last_message, rank, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    sender_id, page_id, message_count, state_labels, state_conversation,
                    conversation_id, is_new_user, thread_id, run_id, state_messages_context,
                    current_time, has_stop_message, last_message, rank, current_time
                ))
                cursor.close()
                conn.commit()
                print(f"DB: Inserted new user state for {sender_id}", file=sys.stderr)
            
            # Return the connection to the pool
            return_db_connection(conn)
            
            return True
            
        except Exception as e:
            print(f"DB ERROR: Failed to save user state to database (attempt {attempt + 1}): {e}", file=sys.stderr)
            
            # Close and return connection if we have one
            try:
                if 'conn' in locals() and conn:
                    return_db_connection(conn)
            except:
                pass
            
            # If this is the last attempt, give up
            if attempt == max_retries - 1:
                print(f"DB ERROR: Failed to save user state after {max_retries} attempts", file=sys.stderr)
                import traceback
                print(traceback.format_exc(), file=sys.stderr)
                return False
            
            # Wait before retrying with exponential backoff
            wait_time = retry_delay * (2 ** attempt)
            print(f"DB: Retrying in {wait_time} seconds...", file=sys.stderr)
            time.sleep(wait_time)
    
    return False