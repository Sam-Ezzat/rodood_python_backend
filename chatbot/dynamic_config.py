"""
Dynamic configuration retrieval module for the chatbot.
This handles fetching configuration from the Node.js server when available
and falls back to local config when needed.
"""

import os
import sys
import json
import requests
import time
from urllib.parse import urljoin

# Import local config as fallback
import config

# Cache for dynamic configuration with TTL
config_cache = {}
config_cache_expiry = {}
access_token_cache = {}  # Separate cache for access tokens
access_token_cache_expiry = {}
CACHE_TTL = 300  # 5 minutes in seconds
TOKEN_CACHE_TTL = 1800  # 30 minutes for access tokens

# Pre-warm cache with critical page data
def warm_cache_for_active_pages():
    """Pre-load configuration for active pages to avoid cold starts"""
    try:
        # Warm cache for known active pages
        active_pages = ["420350114484751"]  # Add more active page IDs here
        for page_id in active_pages:
            fetch_dynamic_config(page_id, force_refresh=True)
        print(f"Cache warmed for {len(active_pages)} active pages", file=sys.stderr)
    except Exception as e:
        print(f"Cache warming failed: {str(e)}", file=sys.stderr)

# Session for connection reuse to improve performance
session = requests.Session()
session.headers.update({'Connection': 'keep-alive'})

def page_exists_in_database(page_id):
    """
    Check if a page exists in the database with fast timeout
    
    :param page_id: The page ID to check
    :return: True if page exists, False otherwise
    """
    try:
        base_url = get_node_server_url()
        if not base_url:
            return False
        
        url = f"{base_url}/api/internal/pages/{page_id}"
        response = session.get(url, timeout=3)  # Fast timeout for existence check
        
        if response.status_code == 200:
            data = response.json()
            return data.get('pageId') == page_id
        
        return False
    except requests.exceptions.Timeout:
        print(f"Fast timeout checking page existence for {page_id} - assuming page exists", file=sys.stderr)
        return True  # Assume page exists if timeout to avoid blocking
    except Exception as e:
        print(f"Error checking page existence for {page_id}: {str(e)}", file=sys.stderr)
        return True  # Assume page exists if error to avoid blocking

def clear_config_cache(page_id=None):
    """
    Clear the configuration cache for a specific page or all pages
    
    :param page_id: The page ID to clear cache for, or None to clear all
    """
    global config_cache, config_cache_expiry
    
    if page_id:
        # Clear cache for a specific page
        cache_key = f"config_{page_id}"
        if cache_key in config_cache:
            del config_cache[cache_key]
        if cache_key in config_cache_expiry:
            del config_cache_expiry[cache_key]
        print(f"Cleared config cache for page {page_id}", file=sys.stderr)
    else:
        # Clear all cache
        config_cache = {}
        config_cache_expiry = {}
        print("Cleared all config cache", file=sys.stderr)

def get_node_server_url():
    """Get the URL for the Node.js server"""
    # The Node.js server runs on port 5000 in this environment
    base_url = 'http://localhost:5000'
    # Only log this during debug mode to reduce log noise
    return base_url 

def fetch_dynamic_config(page_id, force_refresh=False):
    """
    Fetch configuration for a page from the Node.js server
    
    :param page_id: The page ID
    :param force_refresh: Whether to force refresh from the server
    :return: Configuration object
    """
    # Skip page existence check to reduce database calls - 
    # the config endpoint will return defaults if page doesn't exist
    
    cache_key = f"config_{page_id}"
    
    # Check cache first (unless force refresh)
    current_time = time.time()
    if not force_refresh and cache_key in config_cache and current_time < config_cache_expiry.get(cache_key, 0):
        print(f"Using cached config for page {page_id}", file=sys.stderr)
        return config_cache[cache_key]
    
    # Prepare to fetch from Node.js server
    base_url = get_node_server_url()
    if not base_url:
        print(f"No base URL available for Node.js server", file=sys.stderr)
        return None
    
    # Construct the full URL properly
    if base_url.endswith('/'):
        base_url = base_url[:-1]  # Remove trailing slash
    url = f"{base_url}/api/internal/pageconfigs/{page_id}"
    print(f"Fetching config from {url}", file=sys.stderr)
    
    try:
        # Add a reasonable timeout to prevent hanging
        response = session.get(url, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            # Internal endpoint returns data directly, not wrapped in success object
            if data and data.get('pageId'):
                # Store in cache
                config_cache[cache_key] = data
                config_cache_expiry[cache_key] = current_time + CACHE_TTL
                
                print(f"Successfully fetched config for page {page_id} from Node.js server", file=sys.stderr)
                return data
        
        print(f"Failed to fetch config from Node.js, status: {response.status_code}", file=sys.stderr)
    except requests.exceptions.Timeout:
        print(f"Timeout fetching config for page {page_id} - will retry later", file=sys.stderr)
    except Exception as e:
        print(f"Error fetching config from Node.js: {str(e)}", file=sys.stderr)
    
    # No fallback - return None if database fetch fails
    print(f"Failed to fetch config for page {page_id} from database - no fallback available", file=sys.stderr)
    return None

def get_dynamic_assistant_id(page_id):
    """
    Get the assistant ID for a page from database only
    
    :param page_id: The page ID
    :return: Assistant ID or None if not found in database
    """
    try:
        base_url = get_node_server_url()
        if not base_url:
            print(f"No Node.js server URL available", file=sys.stderr)
            return None
        
        url = f"{base_url}/api/internal/pages/{page_id}"
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            assistant_id = data.get('assistantId')
            if assistant_id:
                print(f"Retrieved assistant ID {assistant_id} from database for page {page_id}", file=sys.stderr)
                return assistant_id
        
        print(f"No assistant ID found in database for page {page_id}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error getting assistant ID from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return None

def get_dynamic_access_token(page_id):
    """Get the access token for a page from database with caching and retry logic"""
    # Check cache first
    cache_key = f"token_{page_id}"
    current_time = time.time()
    
    if cache_key in access_token_cache and cache_key in access_token_cache_expiry:
        if current_time < access_token_cache_expiry[cache_key]:
            return access_token_cache[cache_key]
    
    # Try direct database query first
    try:
        from db_helper import execute_query
        query = "SELECT access_token FROM pages WHERE page_id = %s AND access_token IS NOT NULL"
        result = execute_query(query, (page_id,), fetch_one=True)
        
        if result and result[0]:
            access_token = result[0]
            # Cache the access token
            access_token_cache[cache_key] = access_token
            access_token_cache_expiry[cache_key] = current_time + TOKEN_CACHE_TTL
            print(f"Retrieved access token from database for page {page_id}", file=sys.stderr)
            return access_token
    except Exception as db_error:
        print(f"Database query failed for access token: {str(db_error)}", file=sys.stderr)
    
    # Fallback to Node.js API
    max_retries = 2
    timeouts = [10, 15]  # Progressive timeout increases
    
    for attempt in range(max_retries):
        try:
            base_url = get_node_server_url()
            if not base_url:
                print(f"No Node.js server URL available", file=sys.stderr)
                return None
            
            url = f"{base_url}/api/internal/pages/{page_id}"
            timeout = timeouts[attempt] if attempt < len(timeouts) else 15
            
            response = session.get(url, timeout=timeout)
            
            if response.status_code == 200:
                data = response.json()
                access_token = data.get('accessToken')
                if access_token:
                    # Cache the access token
                    access_token_cache[cache_key] = access_token
                    access_token_cache_expiry[cache_key] = current_time + TOKEN_CACHE_TTL
                    print(f"Retrieved and cached access token for page {page_id} (attempt {attempt + 1})", file=sys.stderr)
                    return access_token
            elif response.status_code == 404:
                print(f"Page {page_id} not found in database", file=sys.stderr)
                return None
            
            print(f"No access token found in database for page {page_id}", file=sys.stderr)
            return None
            
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                print(f"Access token request timed out after {max_retries} attempts for page {page_id}", file=sys.stderr)
                return None
            else:
                print(f"Timeout on attempt {attempt + 1}, retrying for page {page_id}", file=sys.stderr)
                time.sleep(1)  # Brief pause before retry
                
        except Exception as e:
            print(f"Error getting access token from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
            return None
    
    return None

def get_dynamic_greeting_message(page_id):
    """Get the greeting message for a page from database only"""
    try:
        base_url = get_node_server_url()
        if not base_url:
            print(f"No Node.js server URL available", file=sys.stderr)
            return ""
        
        url = f"{base_url}/api/internal/pageconfigs/{page_id}"
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            greeting_message = data.get('greetingMessage', "")
            print(f"Retrieved greeting message from database for page {page_id}: '{greeting_message}'", file=sys.stderr)
            return greeting_message
        
        print(f"Failed to get page config from Node.js for page {page_id}, status: {response.status_code}", file=sys.stderr)
        return ""
    except requests.exceptions.RequestException as e:
        print(f"Network error getting page config from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return ""
    except Exception as e:
        print(f"Error getting page config from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return ""

def get_dynamic_first_message(page_id):
    """Get the first message for a page from database only"""
    try:
        base_url = get_node_server_url()
        if not base_url:
            print(f"No Node.js server URL available", file=sys.stderr)
            return ""
        
        url = f"{base_url}/api/internal/pageconfigs/{page_id}"
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            first_message = data.get('firstMessage', "")
            print(f"Retrieved first message from database for page {page_id}: '{first_message}'", file=sys.stderr)
            return first_message
        
        print(f"Failed to get page config from Node.js for page {page_id}, status: {response.status_code}", file=sys.stderr)
        return ""
    except requests.exceptions.RequestException as e:
        print(f"Network error getting page config from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return ""
    except Exception as e:
        print(f"Error getting page config from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return ""

def get_dynamic_max_messages(page_id):
    """Get the maximum number of messages for a page from database only"""
    try:
        base_url = get_node_server_url()
        if not base_url:
            print(f"No Node.js server URL available", file=sys.stderr)
            return 10
        
        url = f"{base_url}/api/internal/pageconfigs/{page_id}"
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            max_messages = data.get('maxMessages', 10)
            print(f"Retrieved max_messages from database for page {page_id}: {max_messages}", file=sys.stderr)
            return max_messages
        
        print(f"Failed to get page config from Node.js for page {page_id}, status: {response.status_code}", file=sys.stderr)
        return 10
    except requests.exceptions.RequestException as e:
        print(f"Network error getting page config from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return 10
    except Exception as e:
        print(f"Error getting page config from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return 10

def get_dynamic_end_message(page_id):
    """Get the end message for a page from database only"""
    try:
        base_url = get_node_server_url()
        if not base_url:
            print(f"No Node.js server URL available", file=sys.stderr)
            return ""
        
        url = f"{base_url}/api/internal/pageconfigs/{page_id}"
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            end_message = data.get('endMessage', "")
            print(f"Retrieved end_message from database for page {page_id}: '{end_message}'", file=sys.stderr)
            return end_message
        
        print(f"Failed to get page config from Node.js for page {page_id}, status: {response.status_code}", file=sys.stderr)
        return ""
    except Exception as e:
        print(f"Error getting page config from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return ""

def get_dynamic_stop_message(page_id):
    """Get the stop message for a page from database only"""
    try:
        base_url = get_node_server_url()
        if not base_url:
            print(f"No Node.js server URL available", file=sys.stderr)
            return ""
        
        url = f"{base_url}/api/internal/pageconfigs/{page_id}"
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            stop_message = data.get('stopMessage', "")
            print(f"Retrieved stop_message from database for page {page_id}: '{stop_message}'", file=sys.stderr)
            return stop_message
        
        print(f"Failed to get page config from Node.js for page {page_id}, status: {response.status_code}", file=sys.stderr)
        return ""
    except Exception as e:
        print(f"Error getting page config from Node.js for page {page_id}: {str(e)}", file=sys.stderr)
        return ""
def get_facebook_page_id_from_instagram_id(instagram_id):
    """
    Get Facebook page ID from Instagram ID mapping using database lookup
    
    :param instagram_id: The Instagram account ID
    :return: Facebook page ID if found, None otherwise
    """
    try:
        base_url = get_node_server_url()
        if not base_url:
            print(f"No Node.js server URL available", file=sys.stderr)
            return None
        
        # For the current database structure where Instagram IDs are in Facebook page metadata
        # We use a hardcoded mapping for the known configuration until we can implement
        # a proper database query through an internal endpoint
        
        # Known mapping based on current database structure
        instagram_to_facebook_mapping = {
            "17841456783426236": "420350114484751"  # Instagram ID to Facebook page ID
        }
        
        if instagram_id in instagram_to_facebook_mapping:
            facebook_page_id = instagram_to_facebook_mapping[instagram_id]
            print(f"Mapped Instagram ID {instagram_id} to Facebook page {facebook_page_id}", file=sys.stderr)
            return facebook_page_id
        
        print(f"No Facebook page mapping found for Instagram ID {instagram_id}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error mapping Instagram ID {instagram_id} to Facebook page: {str(e)}", file=sys.stderr)
        return None

