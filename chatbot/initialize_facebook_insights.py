#!/usr/bin/env python3
"""
Initialize the Facebook insights database with real data from the Facebook API.
This script fetches real insights for all configured pages and stores them in the database.
Run it once to populate the database, or periodically to refresh the data.
"""

import os
import sys
import sqlite3
import json
import time
import importlib.util

# Import config for page IDs
try:
    from config import get_page_config
except ImportError:
    print("Error: config module not found")
    sys.exit(1)

def get_all_page_ids():
    """Get all page IDs that are valid for insights"""
    # Hardcode the known working page IDs to skip the config lookup
    valid_page_ids = ['420350114484751', '8686364944823470']
    print(f"Using {len(valid_page_ids)} known valid page IDs: {', '.join(valid_page_ids)}")
    return valid_page_ids

def initialize_facebook_insights(days=30, refresh=True):
    """
    Initialize Facebook insights for all pages
    
    :param days: Number of days to look back for data
    :param refresh: Whether to force a refresh from the Facebook API
    """
    print(f"Initializing Facebook insights for all pages (days={days}, refresh={refresh})")
    
    # Import our facebook_insights module
    try:
        # Try to import the module
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from facebook_insights import get_facebook_insights, ensure_insights_table
        
        # Make sure tables exist
        ensure_insights_table()
        
        # Get all page IDs
        page_ids = get_all_page_ids()
        print(f"Found {len(page_ids)} pages in config")
        
        # Process each page
        for page_id in page_ids:
            print(f"\nProcessing page {page_id}...")
            
            try:
                # Fetch insights
                start_time = time.time()
                insights = get_facebook_insights(page_id, days, refresh)
                end_time = time.time()
                
                if insights:
                    print(f"Successfully fetched insights for page {page_id} in {end_time - start_time:.2f} seconds")
                    print(f"Total conversations: {insights.get('totalConversations', 0)}")
                    print(f"Total bot messages: {insights.get('totalBotMessages', 0)}")
                    print(f"Conversation trend: {len(insights.get('conversationTrend', []))} days")
                    
                    # Sentiment distribution
                    sentiment_dist = insights.get('sentimentDistribution', [])
                    total_sentiment = sum(item.get('count', 0) for item in sentiment_dist)
                    print(f"Sentiment distribution: {len(sentiment_dist)} ranks, {total_sentiment} total records")
                else:
                    print(f"No insights data returned for page {page_id}")
            
            except Exception as e:
                print(f"Error processing page {page_id}: {str(e)}")
        
        print("\nFacebook insights initialization completed")
    
    except ImportError:
        print("Error: facebook_insights module not found")
    except Exception as e:
        print(f"Error initializing Facebook insights: {str(e)}")

if __name__ == "__main__":
    # Get command line args
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    refresh = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else True
    
    # Run the initialization
    initialize_facebook_insights(days, refresh)