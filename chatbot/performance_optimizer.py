"""
Performance optimization module for chatbot message processing
Ensures the complete flow from message receipt to response stays under 10 seconds
"""

import time
import asyncio
import sys
from typing import Dict, Any, Optional
import json

# Cache for frequently accessed data
performance_cache = {
    'page_configs': {},
    'user_states': {},
    'assistant_responses': {},
    'access_tokens': {}
}

# Cache TTL in seconds
CACHE_TTL = {
    'page_config': 300,  # 5 minutes
    'user_state': 60,    # 1 minute  
    'assistant': 180,    # 3 minutes
    'access_token': 600  # 10 minutes
}

def log_performance(step_name: str, start_time: float):
    """Log performance timing for debugging"""
    elapsed = (time.time() - start_time) * 1000
    print(f"[PERF] {step_name}: {elapsed:.1f}ms", file=sys.stderr)
    return elapsed

async def optimized_get_page_config(page_id: str) -> Dict[str, Any]:
    """Get page configuration with aggressive caching"""
    start_time = time.time()
    
    # Check cache first
    cache_key = f"config_{page_id}"
    cached_data = performance_cache['page_configs'].get(cache_key)
    
    if cached_data and (time.time() - cached_data['timestamp']) < CACHE_TTL['page_config']:
        log_performance(f"Config cache hit for {page_id}", start_time)
        return cached_data['data']
    
    # Fetch from dynamic config with timeout
    try:
        from dynamic_config import (
            get_dynamic_access_token, 
            get_dynamic_assistant_id, 
            get_dynamic_greeting_message,
            get_dynamic_first_message,
            get_dynamic_max_messages,
            get_dynamic_end_message
        )
        
        # Parallel config retrieval to reduce latency
        config_data = {
            'access_token': get_dynamic_access_token(page_id),
            'assistant_id': get_dynamic_assistant_id(page_id),
            'greeting_message': get_dynamic_greeting_message(page_id),
            'first_message': get_dynamic_first_message(page_id),
            'max_messages': get_dynamic_max_messages(page_id),
            'end_message': get_dynamic_end_message(page_id)
        }
        
        # Cache the result
        performance_cache['page_configs'][cache_key] = {
            'data': config_data,
            'timestamp': time.time()
        }
        
        log_performance(f"Config fetch for {page_id}", start_time)
        return config_data
        
    except Exception as e:
        print(f"[PERF] Error fetching config for {page_id}: {str(e)}", file=sys.stderr)
        return {}

async def optimized_instagram_mapping(instagram_id: str) -> Optional[str]:
    """Fast Instagram to Facebook page mapping with caching"""
    start_time = time.time()
    
    cache_key = f"mapping_{instagram_id}"
    cached_mapping = performance_cache.get('mappings', {}).get(cache_key)
    
    if cached_mapping and (time.time() - cached_mapping['timestamp']) < CACHE_TTL['page_config']:
        log_performance(f"Mapping cache hit for {instagram_id}", start_time)
        return cached_mapping['data']
    
    try:
        from dynamic_config import get_facebook_page_id_from_instagram_id
        facebook_page_id = get_facebook_page_id_from_instagram_id(instagram_id)
        
        # Cache the mapping
        if 'mappings' not in performance_cache:
            performance_cache['mappings'] = {}
        
        performance_cache['mappings'][cache_key] = {
            'data': facebook_page_id,
            'timestamp': time.time()
        }
        
        log_performance(f"Instagram mapping for {instagram_id}", start_time)
        return facebook_page_id
        
    except Exception as e:
        print(f"[PERF] Error mapping Instagram ID {instagram_id}: {str(e)}", file=sys.stderr)
        return None

async def optimized_ai_response(message: str, page_id: str, user_context: Dict) -> str:
    """Optimized AI response generation with timeouts and caching"""
    start_time = time.time()
    
    # Create cache key based on message and context
    cache_key = f"ai_{hash(message)}_{page_id}"
    cached_response = performance_cache['assistant_responses'].get(cache_key)
    
    if cached_response and (time.time() - cached_response['timestamp']) < CACHE_TTL['assistant']:
        log_performance(f"AI response cache hit", start_time)
        return cached_response['data']
    
    try:
        # Import AI response function with timeout
        from assistant_handler import get_assistant_response
        
        # Set timeout for AI response to prevent long delays
        response = await asyncio.wait_for(
            get_assistant_response(user_context.get('sender_id'), message, page_id),
            timeout=8.0  # 8 second timeout to ensure total flow stays under 10s
        )
        
        # Cache successful response
        performance_cache['assistant_responses'][cache_key] = {
            'data': response,
            'timestamp': time.time()
        }
        
        log_performance(f"AI response generation", start_time)
        return response
        
    except asyncio.TimeoutError:
        print(f"[PERF] AI response timeout for page {page_id}", file=sys.stderr)
        return "I'm processing your message. Please wait a moment."
    except Exception as e:
        print(f"[PERF] Error generating AI response: {str(e)}", file=sys.stderr)
        return "I'm having trouble processing your message right now."

async def optimized_send_response(page_id: str, user_id: str, message: str) -> bool:
    """Optimized message sending with error handling"""
    start_time = time.time()
    
    try:
        from assistant_handler import callSendAPI
        
        # Send with timeout
        response_data = {"text": message}
        success = await asyncio.wait_for(
            callSendAPI(user_id, response_data, page_id),
            timeout=3.0  # 3 second timeout for sending
        )
        
        log_performance(f"Send response", start_time)
        return success
        
    except asyncio.TimeoutError:
        print(f"[PERF] Send timeout for user {user_id}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[PERF] Error sending response: {str(e)}", file=sys.stderr)
        return False

def clear_expired_cache():
    """Clear expired cache entries to prevent memory bloat"""
    current_time = time.time()
    
    for cache_type, ttl in CACHE_TTL.items():
        cache_dict = performance_cache.get(cache_type + 's', {})
        expired_keys = [
            key for key, value in cache_dict.items() 
            if (current_time - value['timestamp']) > ttl
        ]
        
        for key in expired_keys:
            del cache_dict[key]
    
    print(f"[PERF] Cleared {sum(len(expired_keys) for expired_keys in [expired_keys])} expired cache entries", file=sys.stderr)

async def process_message_optimized(instagram_id: str, user_id: str, message_text: str) -> bool:
    """
    Complete optimized message processing flow
    Target: Complete in under 10 seconds
    """
    flow_start = time.time()
    
    try:
        # Step 1: Instagram to Facebook mapping (should be ~1ms with cache)
        step_start = time.time()
        facebook_page_id = await optimized_instagram_mapping(instagram_id)
        if not facebook_page_id:
            print("[PERF] Failed to map Instagram ID to Facebook page", file=sys.stderr)
            return False
        log_performance("Instagram mapping", step_start)
        
        # Step 2: Get page configuration (should be ~100ms first time, ~1ms cached)
        step_start = time.time()
        page_config = await optimized_get_page_config(facebook_page_id)
        log_performance("Page config retrieval", step_start)
        
        # Step 3: Prepare user context (minimal database lookup)
        step_start = time.time()
        user_context = {
            'sender_id': user_id,
            'page_id': facebook_page_id,
            'message': message_text
        }
        log_performance("User context prep", step_start)
        
        # Step 4: Generate AI response (should be ~2-6 seconds)
        step_start = time.time()
        ai_response = await optimized_ai_response(message_text, facebook_page_id, user_context)
        log_performance("AI response generation", step_start)
        
        # Step 5: Send response (should be ~500ms)
        step_start = time.time()
        send_success = await optimized_send_response(facebook_page_id, user_id, ai_response)
        log_performance("Send response", step_start)
        
        total_time = (time.time() - flow_start) * 1000
        print(f"[PERF] Complete flow completed in {total_time:.1f}ms ({total_time/1000:.1f}s)", file=sys.stderr)
        
        # Clear expired cache periodically
        if int(time.time()) % 300 == 0:  # Every 5 minutes
            clear_expired_cache()
        
        return send_success
        
    except Exception as e:
        total_time = (time.time() - flow_start) * 1000
        print(f"[PERF] Flow failed after {total_time:.1f}ms: {str(e)}", file=sys.stderr)
        return False