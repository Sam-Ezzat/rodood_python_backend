"""
Ultra-fast message handler to ensure sub-10 second response times
Replaces the slow sequential processing with parallel execution and smart caching
"""

import asyncio
import time
import json
import sys
from typing import Dict, Any, Optional, Tuple
import concurrent.futures
import threading

# Global cache to eliminate repeated database lookups
FAST_CACHE = {
    'user_states': {},
    'page_configs': {},
    'db_connections': None,
    'last_cache_clear': time.time()
}

# Lock for thread-safe cache operations
cache_lock = threading.Lock()

def clear_cache_if_needed():
    """Clear cache every 5 minutes to prevent stale data"""
    with cache_lock:
        if time.time() - FAST_CACHE['last_cache_clear'] > 300:  # 5 minutes
            FAST_CACHE['user_states'].clear()
            FAST_CACHE['page_configs'].clear()
            FAST_CACHE['last_cache_clear'] = time.time()
            print("[FAST] Cache cleared", file=sys.stderr)

async def fast_get_user_state(sender_id: str, page_id: str) -> Dict[str, Any]:
    """Get user state with aggressive caching - target: under 50ms"""
    start_time = time.time()
    
    cache_key = f"{sender_id}_{page_id}"
    
    with cache_lock:
        cached_state = FAST_CACHE['user_states'].get(cache_key)
        if cached_state and (time.time() - cached_state['timestamp']) < 60:  # 1 minute cache
            elapsed = (time.time() - start_time) * 1000
            print(f"[FAST] User state cache hit: {elapsed:.1f}ms", file=sys.stderr)
            return cached_state['data']
    
    # Fast database lookup with connection reuse
    try:
        from db_helper import get_db_connection, return_db_connection
        
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            # Single optimized query
            cursor.execute("""
                SELECT message_count, labels, conversation_id, thread_id, run_id, 
                       is_new_user, has_stop_message, last_message, rank, messages_context, conversation
                FROM user_states 
                WHERE sender_id = %s AND page_id = %s
                LIMIT 1
            """, (sender_id, page_id))
            
            user_row = cursor.fetchone()
            cursor.close()
            return_db_connection(conn)
            
            if user_row:
                # Parse JSON fields quickly
                labels = json.loads(user_row[1]) if user_row[1] else []
                messages_context = json.loads(user_row[9]) if user_row[9] else []
                conversation = json.loads(user_row[10]) if user_row[10] else []
                
                user_state = {
                    "page_id": page_id,
                    "message_count": user_row[0],
                    "label": labels,
                    "conversation_id": user_row[2],
                    "thread_id": user_row[3],
                    "run_id": user_row[4],
                    "new_user": user_row[5],
                    "has_stop_message": user_row[6],
                    "last_message": user_row[7],
                    "rank": user_row[8],
                    "messages_context": messages_context,
                    "conversation": conversation
                }
            else:
                # New user - minimal initialization
                user_state = {
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
            
            # Cache the result
            with cache_lock:
                FAST_CACHE['user_states'][cache_key] = {
                    'data': user_state,
                    'timestamp': time.time()
                }
            
            elapsed = (time.time() - start_time) * 1000
            print(f"[FAST] User state DB fetch: {elapsed:.1f}ms", file=sys.stderr)
            return user_state
            
    except Exception as e:
        print(f"[FAST] DB error, using minimal state: {str(e)}", file=sys.stderr)
    
    # Fallback minimal state
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

async def fast_get_page_config(page_id: str) -> Dict[str, Any]:
    """Get page configuration with caching - target: under 100ms"""
    start_time = time.time()
    
    with cache_lock:
        cached_config = FAST_CACHE['page_configs'].get(page_id)
        if cached_config and (time.time() - cached_config['timestamp']) < 300:  # 5 minute cache
            elapsed = (time.time() - start_time) * 1000
            print(f"[FAST] Page config cache hit: {elapsed:.1f}ms", file=sys.stderr)
            return cached_config['data']
    
    # Parallel config retrieval
    try:
        from dynamic_config import (
            get_dynamic_access_token, 
            get_dynamic_assistant_id, 
            get_dynamic_max_messages,
            get_dynamic_end_message
        )
        
        # Get essential config only
        config = {
            'access_token': get_dynamic_access_token(page_id),
            'assistant_id': get_dynamic_assistant_id(page_id),
            'max_messages': get_dynamic_max_messages(page_id) or 10,
            'end_message': get_dynamic_end_message(page_id) or "Thank you for chatting!"
        }
        
        # Cache it
        with cache_lock:
            FAST_CACHE['page_configs'][page_id] = {
                'data': config,
                'timestamp': time.time()
            }
        
        elapsed = (time.time() - start_time) * 1000
        print(f"[FAST] Page config fetch: {elapsed:.1f}ms", file=sys.stderr)
        return config
        
    except Exception as e:
        print(f"[FAST] Config error: {str(e)}", file=sys.stderr)
        return {'access_token': '', 'assistant_id': '', 'max_messages': 10, 'end_message': 'Thank you!'}

async def fast_ai_response(message: str, user_state: Dict, page_id: str) -> str:
    """Fast AI response with timeout protection - target: under 5 seconds"""
    start_time = time.time()
    
    try:
        # Import with timeout protection
        import handeling_User
        
        # Set strict timeout for AI response
        response = await asyncio.wait_for(
            handeling_User.get_chatgpt_response(message, user_state, user_state.get('sender_id', ''), page_id),
            timeout=10.0  # 5 second max for AI
        )
        
        elapsed = (time.time() - start_time) * 1000
        print(f"[FAST] AI response: {elapsed:.1f}ms", file=sys.stderr)
        return response
        
    except asyncio.TimeoutError:
        print("[FAST] AI timeout, using quick response", file=sys.stderr)
        return "I'm processing your message. Please give me a moment to respond properly."
    except Exception as e:
        print(f"[FAST] AI error: {str(e)}", file=sys.stderr)
        return "I'm having trouble right now. Please try again in a moment."

async def fast_send_message(sender_id: str, message: str, page_id: str) -> bool:
    """Fast message sending with timeout - target: under 2 seconds"""
    start_time = time.time()
    
    try:
        from assistant_handler import callSendAPI
        
        # Quick send with timeout
        success = await asyncio.wait_for(
            callSendAPI(sender_id, {"text": message}, page_id),
            timeout=2.0  # 2 second max for sending
        )
        
        elapsed = (time.time() - start_time) * 1000
        print(f"[FAST] Message send: {elapsed:.1f}ms", file=sys.stderr)
        return success
        
    except asyncio.TimeoutError:
        print("[FAST] Send timeout", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[FAST] Send error: {str(e)}", file=sys.stderr)
        return False

async def fast_save_user_state(sender_id: str, user_state: Dict) -> None:
    """Async background save - doesn't block response"""
    try:
        # Use background thread for database save
        def save_to_db():
            try:
                from db_helper import get_db_connection, return_db_connection
                
                conn = get_db_connection()
                if conn:
                    cursor = conn.cursor()
                    
                    # Upsert user state
                    cursor.execute("""
                        INSERT INTO user_states (sender_id, page_id, message_count, labels, conversation_id, 
                                               thread_id, run_id, is_new_user, has_stop_message, last_message, 
                                               rank, messages_context, conversation, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
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
                    
                    conn.commit()
                    cursor.close()
                    return_db_connection(conn)
                    print("[FAST] User state saved to DB", file=sys.stderr)
            except Exception as e:
                print(f"[FAST] Background save error: {str(e)}", file=sys.stderr)
        
        # Run save in background thread
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, save_to_db)
            
    except Exception as e:
        print(f"[FAST] Save state error: {str(e)}", file=sys.stderr)

async def fast_process_message(sender_id: str, message_text: str, page_id: str) -> Tuple[str, bool]:
    """
    Ultra-fast message processing pipeline
    Target: Complete response in under 8 seconds
    """
    total_start = time.time()
    
    try:
        # Clear cache if needed
        clear_cache_if_needed()
        
        # Handle Instagram to Facebook mapping quickly
        if page_id == '17841456783426236':
            page_id = '420350114484751'
        
        print(f"[FAST] Processing message from {sender_id} on page {page_id}", file=sys.stderr)
        
        # Step 1 & 2: Parallel fetch of user state and page config
        user_state_task = fast_get_user_state(sender_id, page_id)
        page_config_task = fast_get_page_config(page_id)
        
        user_state, page_config = await asyncio.gather(user_state_task, page_config_task)
        
        # Update user state quickly
        user_state['message_count'] += 1
        user_state['last_message'] = message_text
        user_state['has_stop_message'] = False
        
        # Check message limits
        if user_state['message_count'] > page_config.get('max_messages', 10):
            end_message = page_config.get('end_message', 'Thank you for chatting!')
            success = await fast_send_message(sender_id, end_message, page_id)
            
            # Reset for new conversation
            user_state['message_count'] = 0
            user_state['has_stop_message'] = True
            
            # Background save
            asyncio.create_task(fast_save_user_state(sender_id, user_state))
            
            total_time = (time.time() - total_start) * 1000
            print(f"[FAST] End message sent in {total_time:.1f}ms", file=sys.stderr)
            return end_message, success
        
        # Step 3: Get AI response with strict timeout
        ai_response = await fast_ai_response(message_text, user_state, page_id)
        
        # Step 4: Send response quickly
        send_success = await fast_send_message(sender_id, ai_response, page_id)
        
        # Step 5: Update context and save in background (don't wait)
        if not user_state.get('messages_context'):
            user_state['messages_context'] = []
        
        user_state['messages_context'].append({"role": "user", "content": message_text})
        user_state['messages_context'].append({"role": "assistant", "content": ai_response})
        
        # Keep only last 10 messages to prevent bloat
        if len(user_state['messages_context']) > 20:
            user_state['messages_context'] = user_state['messages_context'][-20:]
        
        # Background save (doesn't block response)
        asyncio.create_task(fast_save_user_state(sender_id, user_state))
        
        total_time = (time.time() - total_start) * 1000
        print(f"[FAST] Complete processing: {total_time:.1f}ms ({total_time/1000:.1f}s)", file=sys.stderr)
        
        return ai_response, send_success
        
    except Exception as e:
        total_time = (time.time() - total_start) * 1000
        print(f"[FAST] Process failed after {total_time:.1f}ms: {str(e)}", file=sys.stderr)
        
        # Send error response quickly
        error_response = "I'm having technical difficulties. Please try again."
        success = await fast_send_message(sender_id, error_response, page_id)
        return error_response, success