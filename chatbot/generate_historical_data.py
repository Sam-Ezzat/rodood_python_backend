#!/usr/bin/env python3
import sqlite3
import random
import os
import sys
from datetime import datetime, timedelta

"""
Script to generate historical data for the past 30 days
This simulates having data for a month to show trends on the dashboard
"""

def generate_historical_data():
    """Generate historical metrics data for the past 30 days"""
    
    # Connect to the database
    conn = sqlite3.connect('conversation_data.db')
    cursor = conn.cursor()
    
    # Today's date
    today = datetime.now()
    
    # List of page IDs to update
    pages = [

        {
            "id": "1870782619902132",  # Make Hope Last
            "base_conversations": 44,
            "base_messages": 511,
            "activity_level": "medium"
        },
        {
            "id": "420350114484751",   # Rodood Bot
            "base_conversations": 1,
            "base_messages": 7,
            "activity_level": "low"
        },
        {
            "id": "17841456783426236", # rodood.netwrok
            "base_conversations": 1,
            "base_messages": 18,
            "activity_level": "low"
        },
        {
            "id": "609967905535070",   # Majal مچال
            "base_conversations": 0,
            "base_messages": 0,
            "activity_level": "none"
        }
    ]
    
    print(f"Generating historical data for the past 30 days...")
    
    # Loop through past 30 days
    for day_offset in range(30, 0, -1):
        # Calculate the date for this offset
        current_date = today - timedelta(days=day_offset)
        date_str = current_date.strftime('%Y-%m-%d')
        
        # For each page, generate data for this date
        for page in pages:
            page_id = page["id"]
            activity = page["activity_level"]
            
            # Delete any existing data for this date and page
            cursor.execute(
                "DELETE FROM insights_metrics WHERE page_id = ? AND date = ?",
                (page_id, date_str)
            )
            
            # Generate random values based on activity level
            if activity == "high":
                # Start with low values and gradually increase to current values
                progress_factor = day_offset / 30.0
                inverse_progress = 1 - progress_factor
                
                # Base values are multiplied by a factor that increases as we get closer to today
                base_conversations = int(page["base_conversations"] * (0.5 + (0.5 * inverse_progress)))
                base_messages = int(page["base_messages"] * (0.5 + (0.5 * inverse_progress)))
                
                # Add some random variation
                conversations = max(1, base_conversations + random.randint(-5, 5))
                messages = max(4, base_messages + random.randint(-20, 20))
                
            elif activity == "medium":
                # Medium activity pages have more stable numbers with some growth
                progress_factor = day_offset / 30.0
                inverse_progress = 1 - progress_factor
                
                base_conversations = int(page["base_conversations"] * (0.7 + (0.3 * inverse_progress)))
                base_messages = int(page["base_messages"] * (0.7 + (0.3 * inverse_progress)))
                
                conversations = max(1, base_conversations + random.randint(-3, 3))
                messages = max(4, base_messages + random.randint(-10, 10))
                
            elif activity == "low":
                # Low activity pages have sporadic engagement
                if random.random() < 0.3:  # 30% chance of activity
                    conversations = 1
                    messages = random.randint(3, 10)
                else:
                    conversations = 0
                    messages = 0
                    
            else:  # "none"
                # Inactive pages have no messages
                conversations = 0
                messages = 0
            
            # Calculate bot messages (approximately 80% of all messages)
            bot_messages = int(messages * 0.8)
            
            # Generate a slightly random sentiment score (3.0 is neutral)
            sentiment_score = min(5.0, max(1.0, 3.0 + (random.random() - 0.5)))
            
            # Insert the historical data
            cursor.execute(
                """
                INSERT INTO insights_metrics 
                (page_id, date, unique_users, total_messages, bot_messages, avg_sentiment_score) 
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (page_id, date_str, conversations, messages, bot_messages, sentiment_score)
            )
            
            print(f"Added historical data for page {page_id} on {date_str}: {conversations} conversations, {messages} messages")
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print("Historical data generation completed!")

if __name__ == "__main__":
    generate_historical_data()