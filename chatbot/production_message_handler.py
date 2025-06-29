"""
Production-ready message handler that fixes the critical issues causing 30-50 second delays
Addresses: database connection errors, race conditions, timeout issues, and crash recovery
"""

import asyncio
import time
import json
import sys
from typing import Dict, Any, Optional, Tuple
import threading
import uuid
from contextlib import asynccontextmanager

# Global state for production message handling
PRODUCTION_STATE = {
    'active_requests': {},
    'connection_pool': None,
    'last_pool_reset': time.time(),
    'error_count': 0,
    'lock': threading.Lock()
}

def log_production(message: str, level: str = "INFO"):
    """Production logging with timestamps"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[PROD-{level}] {timestamp} - {message}", file=sys.stderr)

async def get_robust_db_connection():
    """Get database connection with automatic retry and pool management"""
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            from db_helper import get_db_connection, return_db_connection
            
            conn = get_db_connection()
            if conn:
                # Test connection with a simple query
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return conn
                
        except Exception as e:
            log_production(f"DB connection attempt {attempt + 1} failed: {str(e)}", "WARNING")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                log_production("All DB connection attempts failed", "ERROR")
                return None
    
    return None

@asynccontextmanager
async def robust_db_transaction():
    """Context manager for robust database transactions"""
    conn = None
    cursor = None
    try:
        conn = await get_robust_db_connection()
        if conn:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        else:
            yield None
    except Exception as e:
        log_production(f"Database transaction error: {str(e)}", "ERROR")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        yield None
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                from db_helper import return_db_connection
                return_db_connection(conn)
            except:
                pass

async def get_user_state_robust(sender_id: str, page_id: str) -> Dict[str, Any]:
    """Get user state with robust error handling and defaults"""
    start_time = time.time()
    
    try:
        async with robust_db_transaction() as cursor:
            if cursor:
                cursor.execute("""
                    SELECT message_count, labels, conversation_id, thread_id, run_id, 
                           is_new_user, has_stop_message, last_message, rank, 
                           messages_context, conversation
                    FROM user_states 
                    WHERE sender_id = %s AND page_id = %s
                    LIMIT 1
                """, (sender_id, page_id))
                
                user_row = cursor.fetchone()
                
                if user_row:
                    # Safely parse JSON fields
                    try:
                        labels = json.loads(user_row[1]) if user_row[1] else []
                        messages_context = json.loads(user_row[9]) if user_row[9] else []
                        conversation = json.loads(user_row[10]) if user_row[10] else []
                    except (json.JSONDecodeError, TypeError):
                        labels = []
                        messages_context = []
                        conversation = []
                    
                    elapsed = (time.time() - start_time) * 1000
                    log_production(f"User state loaded in {elapsed:.1f}ms")
                    
                    return {
                        "page_id": page_id,
                        "message_count": user_row[0] or 0,
                        "label": labels,
                        "conversation_id": user_row[2],
                        "thread_id": user_row[3],
                        "run_id": user_row[4],
                        "new_user": user_row[5] if user_row[5] is not None else True,
                        "has_stop_message": user_row[6] if user_row[6] is not None else False,
                        "last_message": user_row[7] or "",
                        "rank": user_row[8],
                        "messages_context": messages_context,
                        "conversation": conversation
                    }
    
    except Exception as e:
        log_production(f"Error loading user state: {str(e)}", "ERROR")
    
    # Return default state if database fails
    elapsed = (time.time() - start_time) * 1000
    log_production(f"Using default user state after {elapsed:.1f}ms")
    
    return {
        "page_id": page_id,
        "message_count": 0,
        "label": [],
        "conversation": [],
        "conversation_id": None,
        "new_user": True,
        "thread_id": None,
        "run_id": None,
        "messages_context": [],
        "has_stop_message": False,
        "last_message": "",
        "rank": None
    }

async def get_page_config_robust(page_id: str) -> Dict[str, Any]:
    """Get page configuration with robust error handling"""
    start_time = time.time()
    
    try:
        from dynamic_config import (
            get_dynamic_access_token, 
            get_dynamic_assistant_id, 
            get_dynamic_max_messages,
            get_dynamic_end_message
        )
        
        # Get essential config with timeouts
        config = {}
        
        # Try to get each config value with individual error handling
        try:
            config['access_token'] = get_dynamic_access_token(page_id) or ""
        except Exception as e:
            log_production(f"Error getting access token: {str(e)}", "WARNING")
            config['access_token'] = ""
        
        try:
            config['assistant_id'] = get_dynamic_assistant_id(page_id) or ""
        except Exception as e:
            log_production(f"Error getting assistant ID: {str(e)}", "WARNING")
            config['assistant_id'] = ""
        
        try:
            config['max_messages'] = get_dynamic_max_messages(page_id) or 10
        except Exception as e:
            log_production(f"Error getting max messages: {str(e)}", "WARNING")
            config['max_messages'] = 10
        
        try:
            config['end_message'] = get_dynamic_end_message(page_id) or "Thank you for chatting!"
        except Exception as e:
            log_production(f"Error getting end message: {str(e)}", "WARNING")
            config['end_message'] = "Thank you for chatting!"
        
        elapsed = (time.time() - start_time) * 1000
        log_production(f"Page config loaded in {elapsed:.1f}ms")
        return config
        
    except Exception as e:
        log_production(f"Critical error getting page config: {str(e)}", "ERROR")
        
        # Return minimal working config
        return {
            'access_token': "",
            'assistant_id': "",
            'max_messages': 10,
            'end_message': "Thank you for chatting!"
        }

async def get_ai_response_robust(message: str, user_state: Dict, page_id: str) -> str:
    """Get AI response with robust error handling and timeouts"""
    start_time = time.time()
    
    try:
        # Set very strict timeout to prevent hanging
        import handeling_User
        
        response = await asyncio.wait_for(
            handeling_User.get_chatgpt_response(
                message, 
                user_state, 
                user_state.get('sender_id', ''), 
                page_id
            ),
            timeout=6.0  # 6 second max timeout
        )
        
        elapsed = (time.time() - start_time) * 1000
        log_production(f"AI response generated in {elapsed:.1f}ms")
        
        # Handle different response types
        if isinstance(response, dict):
            response = response.get('message', response.get('content', str(response)))
        elif not isinstance(response, str):
            response = str(response)
        
        # Ensure response is not empty
        if not response or response.strip() == "":
            return "I understand your message. Let me help you with that."
        
        return response
        
    except asyncio.TimeoutError:
        log_production("AI response timeout - using fallback", "WARNING")
        return "I'm processing your message. Please give me a moment to respond properly."
    except Exception as e:
        log_production(f"AI response error: {str(e)}", "ERROR")
        return "I'm having technical difficulties. Please try again in a moment."

async def send_message_robust(sender_id: str, message: str, page_id: str) -> bool:
    """Send message with robust error handling"""
    start_time = time.time()
    
    if not message or message.strip() == "":
        log_production("Attempted to send empty message", "WARNING")
        return False
    
    try:
        from assistant_handler import callSendAPI
        
        # Send with timeout and retry logic
        success = await asyncio.wait_for(
            callSendAPI(sender_id, {"text": message}, page_id),
            timeout=3.0  # 3 second max for sending
        )
        
        elapsed = (time.time() - start_time) * 1000
        log_production(f"Message sent in {elapsed:.1f}ms, success: {success}")
        return success
        
    except asyncio.TimeoutError:
        log_production("Message send timeout", "WARNING")
        return False
    except Exception as e:
        log_production(f"Message send error: {str(e)}", "ERROR")
        return False

async def save_user_state_robust(sender_id: str, user_state: Dict) -> None:
    """Save user state with robust error handling (background task)"""
    try:
        async with robust_db_transaction() as cursor:
            if cursor:
                # Use a more robust upsert query
                cursor.execute("""
                    INSERT INTO user_states (
                        sender_id, page_id, message_count, labels, conversation_id, 
                        thread_id, run_id, is_new_user, has_stop_message, last_message, 
                        rank, messages_context, conversation, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (sender_id, page_id) 
                    DO UPDATE SET 
                        message_count = EXCLUDED.message_count,
                        labels = EXCLUDED.labels,
                        conversation_id = EXCLUDED.conversation_id,
                        thread_id = EXCLUDED.thread_id,
                        run_id = EXCLUDED.run_id,
                        is_new_user = EXCLUDED.is_new_user,
                        has_stop_message = EXCLUDED.has_stop_message,
                        last_message = EXCLUDED.last_message,
                        rank = EXCLUDED.rank,
                        messages_context = EXCLUDED.messages_context,
                        conversation = EXCLUDED.conversation,
                        updated_at = NOW()
                """, (
                    sender_id, user_state['page_id'], user_state['message_count'],
                    json.dumps(user_state['label']), user_state['conversation_id'],
                    user_state['thread_id'], user_state['run_id'], user_state['new_user'],
                    user_state['has_stop_message'], user_state['last_message'],
                    user_state['rank'], json.dumps(user_state['messages_context']),
                    json.dumps(user_state['conversation'])
                ))
                
                log_production("User state saved successfully")
                
    except Exception as e:
        log_production(f"Error saving user state: {str(e)}", "ERROR")

async def process_message_production(sender_id: str, message_text: str, page_id: str) -> Tuple[str, bool]:
    """
    Production-ready message processing with comprehensive error handling
    Target: Complete response in under 8 seconds with high reliability
    """
    total_start = time.time()
    request_id = str(uuid.uuid4())[:8]
    
    log_production(f"[{request_id}] Starting message processing for {sender_id}")
    
    try:
        # Handle Instagram to Facebook mapping
        if page_id == '17841456783426236':
            page_id = '420350114484751'
            log_production(f"[{request_id}] Mapped Instagram to Facebook page")
        
        # Parallel execution of independent operations
        user_state_task = get_user_state_robust(sender_id, page_id)
        page_config_task = get_page_config_robust(page_id)
        
        user_state, page_config = await asyncio.gather(
            user_state_task, 
            page_config_task,
            return_exceptions=True
        )
        
        # Handle any exceptions from parallel tasks
        if isinstance(user_state, Exception):
            log_production(f"[{request_id}] User state error: {str(user_state)}", "ERROR")
            user_state = await get_user_state_robust(sender_id, page_id)
        
        if isinstance(page_config, Exception):
            log_production(f"[{request_id}] Page config error: {str(page_config)}", "ERROR")
            page_config = await get_page_config_robust(page_id)
        
        # Update user state
        user_state['message_count'] += 1
        user_state['last_message'] = message_text
        user_state['has_stop_message'] = False
        user_state['sender_id'] = sender_id  # Ensure sender_id is in state
        
        # Check message limits
        max_messages = page_config.get('max_messages', 10)
        if user_state['message_count'] > max_messages:
            end_message = page_config.get('end_message', 'Thank you for chatting!')
            success = await send_message_robust(sender_id, end_message, page_id)
            
            # Reset for new conversation
            user_state['message_count'] = 0
            user_state['has_stop_message'] = True
            
            # Background save
            asyncio.create_task(save_user_state_robust(sender_id, user_state))
            
            total_time = (time.time() - total_start) * 1000
            log_production(f"[{request_id}] End message sent in {total_time:.1f}ms")
            return end_message, success
        
        # Get AI response
        ai_response = await get_ai_response_robust(message_text, user_state, page_id)
        
        # Send response
        send_success = await send_message_robust(sender_id, ai_response, page_id)
        
        # Update conversation context (keep last 10 messages only)
        if not user_state.get('messages_context'):
            user_state['messages_context'] = []
        
        user_state['messages_context'].append({"role": "user", "content": message_text})
        user_state['messages_context'].append({"role": "assistant", "content": ai_response})
        
        # Limit context size to prevent bloat
        if len(user_state['messages_context']) > 20:
            user_state['messages_context'] = user_state['messages_context'][-20:]
        
        # Background save (doesn't block response)
        asyncio.create_task(save_user_state_robust(sender_id, user_state))
        
        total_time = (time.time() - total_start) * 1000
        log_production(f"[{request_id}] Complete processing: {total_time:.1f}ms ({total_time/1000:.1f}s)")
        
        return ai_response, send_success
        
    except Exception as e:
        total_time = (time.time() - total_start) * 1000
        log_production(f"[{request_id}] Critical error after {total_time:.1f}ms: {str(e)}", "ERROR")
        
        # Emergency fallback response
        error_response = "I'm experiencing technical difficulties. Please try again."
        try:
            success = await send_message_robust(sender_id, error_response, page_id)
            return error_response, success
        except:
            return error_response, False