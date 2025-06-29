#!/usr/bin/env python3
import sqlite3
import sys
import os
from datetime import datetime, timedelta
import random

"""
Script to simulate daily metrics changes for the dashboard
This would be replaced by real conversation tracking in production

In a real-world scenario, this script would:
1. Be scheduled to run daily (via cron or similar)
2. Calculate real metrics based on actual conversations in the database
3. Store a daily snapshot in the insights_metrics table
"""

def update_daily_metrics():
    """Update daily metrics for all pages"""
    
    # Connect to the database
    conn = sqlite3.connect('conversation_data.db')
    cursor = conn.cursor()
    
    # Get yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    
    # List of page IDs to update
    pages = [

        "1870782619902132",  # Make Hope Last
        "420350114484751",   # Rodood Bot
        "17841456783426236", # rodood.netwrok
        "609967905535070"    # Majal مچال
    ]
    
    print(f"Updating daily metrics for {today}...")
    
    # For each page, get the previous day's metrics and add a small random change
    for page_id in pages:
        # Get the most recent metrics for this page
        cursor.execute(
            """
            SELECT unique_users, total_messages, bot_messages, avg_sentiment_score 
            FROM insights_metrics 
            WHERE page_id = ? 
            ORDER BY date DESC 
            LIMIT 1
            """, 
            (page_id,)
        )
        row = cursor.fetchone()
        
        if not row:
            # No previous metrics for this page, skip
            print(f"No previous metrics for page {page_id}, skipping")
            continue
        
        # Unpack previous metrics
        prev_users, prev_total_msgs, prev_bot_msgs, prev_sentiment = row
        
        # Calculate small random changes based on page activity level
        if prev_users > 100:  # Very active page
            new_conversations = random.randint(3, 8)
            new_messages = random.randint(15, 40)
        elif prev_users > 30:  # Moderately active
            new_conversations = random.randint(1, 3)
            new_messages = random.randint(5, 20)
        elif prev_users > 0:   # Low activity
            new_conversations = random.randint(0, 1)
            new_messages = random.randint(0, 5)
        else:  # No activity
            new_conversations = 0
            new_messages = 0
        
        # Calculate new metrics
        new_users = prev_users + new_conversations
        new_total_msgs = prev_total_msgs + new_messages
        new_bot_msgs = prev_bot_msgs + int(new_messages * 0.8)  # Assume 80% of messages are from bot
        
        # Random small change to sentiment (-0.2 to +0.2)
        sentiment_change = (random.random() * 0.4) - 0.2
        new_sentiment = max(1.0, min(5.0, prev_sentiment + sentiment_change))
        
        # Insert new record for today
        cursor.execute(
            """
            INSERT INTO insights_metrics 
            (page_id, date, unique_users, total_messages, bot_messages, avg_sentiment_score) 
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (page_id, today, new_users, new_total_msgs, new_bot_msgs, new_sentiment)
        )
        
        print(f"Updated page {page_id}: {new_conversations} new conversations, {new_messages} new messages")
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print("Daily metrics update completed!")

if __name__ == "__main__":
    update_daily_metrics()