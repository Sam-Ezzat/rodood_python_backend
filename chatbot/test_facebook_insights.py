#!/usr/bin/env python3
"""
Test script for the new facebook_insights module.
This will test fetching real data from the Facebook API.
Run it with a page ID: python test_facebook_insights.py <page_id>
"""

import sys
import json
import os
import importlib.util

def main():
    # Check if page_id was provided
    if len(sys.argv) < 2:
        print("Usage: python test_facebook_insights.py <page_id> [days] [refresh]")
        sys.exit(1)
    
    page_id = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    refresh = True if len(sys.argv) > 3 and sys.argv[3].lower() == 'true' else False
    
    print(f"Testing Facebook insights for page {page_id} with days={days}, refresh={refresh}")
    
    # Import our facebook_insights module
    try:
        # Make sure we're in the right directory
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        # Try to import the module
        from facebook_insights import get_facebook_insights
        
        # Fetch insights
        print("Calling get_facebook_insights...")
        insights = get_facebook_insights(page_id, days, refresh)
        
        if insights:
            print("\nSuccessfully fetched Facebook insights!")
            print(json.dumps(insights, indent=2))
            
            # Print a summary
            print("\nSummary:")
            print(f"Total conversations: {insights.get('totalConversations', 0)}")
            print(f"Total bot messages: {insights.get('totalBotMessages', 0)}")
            print(f"Avg response time: {insights.get('averageResponseTime', 0)}")
            print(f"Completion rate: {insights.get('completionRate', 0)}")
            print(f"Conversation trend: {len(insights.get('conversationTrend', []))} days")
            
            # Analyze sentiment distribution
            sentiment_dist = insights.get('sentimentDistribution', [])
            total_sentiment = sum(item.get('count', 0) for item in sentiment_dist)
            print(f"Sentiment distribution: {len(sentiment_dist)} ranks, {total_sentiment} total records")
            
            # Verify that all fields are present
            required_fields = ['totalConversations', 'totalBotMessages', 
                              'averageResponseTime', 'completionRate',
                              'conversationTrend', 'sentimentDistribution']
            
            missing_fields = [field for field in required_fields if field not in insights]
            if missing_fields:
                print(f"Warning: Missing fields: {', '.join(missing_fields)}")
            else:
                print("All required fields are present")
        else:
            print("No insights data returned")
    
    except ImportError:
        print("Error: facebook_insights module not found")
    except Exception as e:
        print(f"Error testing Facebook insights: {str(e)}")

if __name__ == "__main__":
    main()