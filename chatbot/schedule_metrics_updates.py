#!/usr/bin/env python3
import time
import os
import sys
import datetime
import threading
import schedule

"""
Scheduler script to run daily metrics updates
This script should be started once and will run continuously,
triggering the update_daily_metrics function at midnight each day.
"""

def run_scheduled_job():
    """Run the update_daily_metrics function from insights_storage"""
    print(f"Running scheduled metrics update at {datetime.datetime.now()}", file=sys.stderr)
    
    try:
        # Import the function dynamically to avoid circular imports
        from insights_storage import update_daily_metrics
        
        # Run the update function
        success = update_daily_metrics()
        
        if success:
            print("Daily metrics update completed successfully", file=sys.stderr)
        else:
            print("Daily metrics update failed", file=sys.stderr)
            
    except Exception as e:
        print(f"Error running scheduled metrics update: {str(e)}", file=sys.stderr)

def run_threaded(job_func):
    """Run job in a separate thread"""
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def main():
    """Main function that sets up and runs the scheduler"""
    print("Starting metrics update scheduler...", file=sys.stderr)
    
    # Schedule the job to run at midnight every day
    schedule.every().day.at("00:00").do(run_threaded, run_scheduled_job)
    
    # Also run once at startup to ensure we have today's data
    print("Running initial metrics update...", file=sys.stderr)
    run_scheduled_job()
    
    # Keep the script running and check for scheduled jobs
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()