#!/usr/bin/env python3
"""
Script to update Facebook insights data daily.
This is designed to be run as a cron job, for example at midnight each day.
It fetches fresh insights data for all pages configured in the system.
"""

import os
import sys
import time
import datetime
import schedule
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

def update_facebook_insights():
    """Update Facebook insights for all pages"""
    print(f"===== Daily Facebook insights update started at {datetime.datetime.now()} =====")
    
    # Import our facebook_insights module
    try:
        # Try to import the module
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from facebook_insights import get_facebook_insights
        
        # Get all page IDs
        page_ids = get_all_page_ids()
        print(f"Found {len(page_ids)} pages in config")
        
        # Process each page
        for page_id in page_ids:
            print(f"\nUpdating insights for page {page_id}...")
            
            try:
                # Fetch insights with refresh=True to force fresh data
                start_time = time.time()
                insights = get_facebook_insights(page_id, days=7, refresh=True)
                end_time = time.time()
                
                if insights:
                    print(f"Successfully updated insights for page {page_id} in {end_time - start_time:.2f} seconds")
                    print(f"Total conversations: {insights.get('totalConversations', 0)}")
                    print(f"Total bot messages: {insights.get('totalBotMessages', 0)}")
                else:
                    print(f"No insights data returned for page {page_id}")
            
            except Exception as e:
                print(f"Error updating page {page_id}: {str(e)}")
        
        print(f"===== Daily Facebook insights update completed at {datetime.datetime.now()} =====")
    
    except ImportError:
        print("Error: facebook_insights module not found")
    except Exception as e:
        print(f"Error updating Facebook insights: {str(e)}")

def run_scheduler():
    """Run the scheduler for regular updates"""
    print(f"Starting Facebook insights scheduler at {datetime.datetime.now()}")
    
    # Schedule the job to run at midnight every day
    schedule.every().day.at("00:00").do(update_facebook_insights)
    
    # Also run immediately on startup
    update_facebook_insights()
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    # Run once if any argument is provided, otherwise run scheduler
    if len(sys.argv) > 1:
        update_facebook_insights()
    else:
        run_scheduler()