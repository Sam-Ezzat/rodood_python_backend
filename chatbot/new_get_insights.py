def get_insights():
    """
    Endpoint for getting insights data from the database
    Expected payload: { page_id: string, days: number }
    """
    try:
        data = request.json
        page_id = data.get('page_id')
        days = int(data.get('days', 7))

        if not page_id:
            return jsonify({
                'success': False,
                'error': 'Missing page_id in request'
            }), 400
        
        # Enhanced caching with in-flight request tracking
        cache_key = f"{page_id}_{days}"
        current_time = time.time()
        
        # Use cache if available and less than 15 minutes old (TTL-based caching)
        if cache_key in insights_cache and insights_cache_expiry.get(cache_key, 0) > current_time:
            print(f"Using cached insights for page {page_id}", file=sys.stderr)
            return jsonify(insights_cache[cache_key])
            
        # Check if there's already an in-flight request for this cache key
        # This prevents duplicate API calls when multiple users request the same data simultaneously
        if cache_key in inflight_requests:
            last_request_time = inflight_requests.get(cache_key, 0)
            # Only consider requests in-flight if they started less than 10 seconds ago
            if current_time - last_request_time < 10:
                print(f"Request for {cache_key} already in flight, returning stale cache or minimal data", file=sys.stderr)
                # Return stale cache if available, otherwise return minimal data
                if cache_key in insights_cache:
                    return jsonify(insights_cache[cache_key])
                else:
                    # Return minimal data structure that client can render
                    return jsonify({
                        'success': True, 
                        'data': {
                            'totalConversations': 0,
                            'totalBotMessages': 0,
                            'averageResponseTime': 0,
                            'completionRate': 0,
                            'conversationTrend': [],
                            'sentimentDistribution': [
                                {'rank': i, 'count': 0} for i in range(1, 6)
                            ]
                        },
                        'status': 'processing'
                    })
        
        # Mark this request as in-flight
        inflight_requests[cache_key] = current_time
            
        # Map Instagram page ID to Facebook page ID if needed
        original_id = page_id
        mapped_id = get_page_id_from_instagram_id(page_id)
        if mapped_id != page_id:
            page_id = mapped_id
            print(f"Instagram page ID {original_id} detected in insights, mapping to Facebook page ID {page_id}", file=sys.stderr)

        try:
            # Import the insights_storage module
            try:
                import importlib.util
                spec = importlib.util.find_spec('insights_storage')
                if spec is None:
                    # If module not found, try direct import
                    import insights_storage
                else:
                    insights_storage = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(insights_storage)
                    
                print(f"Fetching insights from database for page {page_id}", file=sys.stderr)
                insights_data = insights_storage.get_insights_metrics(page_id, days)
                
            except ImportError:
                print(f"Insights storage module not found, using sentiment_storage", file=sys.stderr)
                # Get sentiment distribution directly
                from sentiment import get_sentiment_distribution
                sentiment_distribution = get_sentiment_distribution(page_id, days)
                
                # Get total conversations from sentiments
                total_count = sum(item['count'] for item in sentiment_distribution)
                
                # Estimate other metrics based on the sentiment data
                insights_data = {
                    'totalConversations': max(1, total_count),
                    'totalBotMessages': max(3, total_count * 4),  # Assume 4 bot messages per conversation
                    'averageResponseTime': 45.0,  # Default value
                    'completionRate': 0.85,  # Default 85% completion
                    'conversationTrend': [],  # Empty trend data
                    'sentimentDistribution': sentiment_distribution
                }
                
                # Generate basic trend data
                end_date = datetime.datetime.now()
                start_date = end_date - datetime.timedelta(days=days)
                
                # Create a flat trend with all conversations on first day
                for i in range(days):
                    date = (start_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                    count = total_count if i == 0 else 0
                    insights_data['conversationTrend'].append({'date': date, 'count': count})
            
            # Prepare response
            response_data = {'success': True, 'data': insights_data}
            
            # Cache the response for 15 minutes
            insights_cache[cache_key] = response_data
            insights_cache_expiry[cache_key] = current_time + (15 * 60)  # 15 minutes
            
            # Remove this request from in-flight tracking
            if cache_key in inflight_requests:
                del inflight_requests[cache_key]
            
            return jsonify(response_data)
            
        except Exception as e:
            print(f"Error fetching insights from database: {str(e)}", file=sys.stderr)
            
            # Clean up in-flight request tracking even on error
            if cache_key in inflight_requests:
                del inflight_requests[cache_key]
                
            return jsonify({
                'success': False,
                'error': f"Failed to get insights: {str(e)}"
            }), 500

    except Exception as e:
        print(f"Error in insights API: {str(e)}", file=sys.stderr)
        
        # Also clean up in-flight request if we have the cache_key
        try:
            if 'cache_key' in locals() and cache_key in inflight_requests:
                del inflight_requests[cache_key]
        except:
            pass  # Don't let cleanup errors hide the original error
            
        return jsonify({'success': False, 'error': str(e)}), 500