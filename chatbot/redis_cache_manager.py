"""
Redis Cache Manager for high-performance chatbot operations
Handles conversation state, page configurations, and AI session caching
"""

import redis
import json
import time
import sys
from typing import Dict, Any, Optional, List
import os

class RedisCacheManager:
    def __init__(self):
        self.redis_client = None
        self.connected = False
        self.connect()
    
    def connect(self):
        """Connect to Redis with fallback to local instance"""
        try:
            # Try to connect to Redis
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
            self.redis_client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # Test connection
            self.redis_client.ping()
            self.connected = True
            print("[REDIS] Connected successfully", file=sys.stderr)
            
        except Exception as e:
            print(f"[REDIS] Connection failed: {str(e)}", file=sys.stderr)
            self.connected = False
    
    def is_connected(self) -> bool:
        """Check if Redis is connected and responsive"""
        try:
            if self.redis_client and self.connected:
                self.redis_client.ping()
                return True
        except:
            self.connected = False
        return False
    
    # Conversation State Management
    def cache_conversation_state(self, conversation_id: str, state: Dict[str, Any], ttl: int = 3600):
        """Cache conversation state with 1 hour TTL"""
        if not self.is_connected():
            return False
        
        try:
            key = f"conv_state:{conversation_id}"
            state_json = json.dumps(state)
            self.redis_client.setex(key, ttl, state_json)
            return True
        except Exception as e:
            print(f"[REDIS] Error caching conversation state: {str(e)}", file=sys.stderr)
            return False
    
    def get_conversation_state(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve conversation state from cache"""
        if not self.is_connected():
            return None
        
        try:
            key = f"conv_state:{conversation_id}"
            state_json = self.redis_client.get(key)
            if state_json:
                return json.loads(state_json)
        except Exception as e:
            print(f"[REDIS] Error getting conversation state: {str(e)}", file=sys.stderr)
        return None
    
    # Page Configuration Caching
    def cache_page_config(self, page_id: str, config: Dict[str, Any], ttl: int = 1800):
        """Cache page configuration with 30 minute TTL"""
        if not self.is_connected():
            return False
        
        try:
            key = f"page_config:{page_id}"
            config_json = json.dumps(config)
            self.redis_client.setex(key, ttl, config_json)
            return True
        except Exception as e:
            print(f"[REDIS] Error caching page config: {str(e)}", file=sys.stderr)
            return False
    
    def get_page_config(self, page_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve page configuration from cache"""
        if not self.is_connected():
            return None
        
        try:
            key = f"page_config:{page_id}"
            config_json = self.redis_client.get(key)
            if config_json:
                return json.loads(config_json)
        except Exception as e:
            print(f"[REDIS] Error getting page config: {str(e)}", file=sys.stderr)
        return None
    
    # AI Session Caching
    def cache_ai_session(self, conversation_id: str, session_data: Dict[str, Any], ttl: int = 7200):
        """Cache AI session data with 2 hour TTL"""
        if not self.is_connected():
            return False
        
        try:
            key = f"ai_session:{conversation_id}"
            session_json = json.dumps(session_data)
            self.redis_client.setex(key, ttl, session_json)
            return True
        except Exception as e:
            print(f"[REDIS] Error caching AI session: {str(e)}", file=sys.stderr)
            return False
    
    def get_ai_session(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve AI session data from cache"""
        if not self.is_connected():
            return None
        
        try:
            key = f"ai_session:{conversation_id}"
            session_json = self.redis_client.get(key)
            if session_json:
                return json.loads(session_json)
        except Exception as e:
            print(f"[REDIS] Error getting AI session: {str(e)}", file=sys.stderr)
        return None
    
    # Message Rate Limiting
    def check_rate_limit(self, sender_id: str, page_id: str, limit: int = 10, window: int = 60) -> bool:
        """Check if sender is within rate limits (10 messages per minute by default)"""
        if not self.is_connected():
            return True  # Allow if Redis unavailable
        
        try:
            key = f"rate_limit:{page_id}:{sender_id}"
            current_count = self.redis_client.get(key)
            
            if current_count is None:
                # First message in window
                self.redis_client.setex(key, window, 1)
                return True
            
            current_count = int(current_count)
            if current_count >= limit:
                return False  # Rate limit exceeded
            
            # Increment counter
            self.redis_client.incr(key)
            return True
            
        except Exception as e:
            print(f"[REDIS] Error checking rate limit: {str(e)}", file=sys.stderr)
            return True  # Allow if error
    
    # Platform Mapping Cache
    def cache_platform_mapping(self, source_id: str, target_id: str, ttl: int = 86400):
        """Cache platform mapping (Instagram to Facebook) with 24 hour TTL"""
        if not self.is_connected():
            return False
        
        try:
            key = f"platform_map:{source_id}"
            self.redis_client.setex(key, ttl, target_id)
            return True
        except Exception as e:
            print(f"[REDIS] Error caching platform mapping: {str(e)}", file=sys.stderr)
            return False
    
    def get_platform_mapping(self, source_id: str) -> Optional[str]:
        """Get platform mapping from cache"""
        if not self.is_connected():
            return None
        
        try:
            key = f"platform_map:{source_id}"
            return self.redis_client.get(key)
        except Exception as e:
            print(f"[REDIS] Error getting platform mapping: {str(e)}", file=sys.stderr)
            return None
    
    # Analytics and Metrics Caching
    def cache_daily_metrics(self, page_id: str, date: str, metrics: Dict[str, Any], ttl: int = 3600):
        """Cache daily metrics with 1 hour TTL"""
        if not self.is_connected():
            return False
        
        try:
            key = f"metrics:{page_id}:{date}"
            metrics_json = json.dumps(metrics)
            self.redis_client.setex(key, ttl, metrics_json)
            return True
        except Exception as e:
            print(f"[REDIS] Error caching metrics: {str(e)}", file=sys.stderr)
            return False
    
    def get_daily_metrics(self, page_id: str, date: str) -> Optional[Dict[str, Any]]:
        """Get daily metrics from cache"""
        if not self.is_connected():
            return None
        
        try:
            key = f"metrics:{page_id}:{date}"
            metrics_json = self.redis_client.get(key)
            if metrics_json:
                return json.loads(metrics_json)
        except Exception as e:
            print(f"[REDIS] Error getting metrics: {str(e)}", file=sys.stderr)
        return None
    
    # Cache Management
    def invalidate_page_cache(self, page_id: str):
        """Invalidate all cache entries for a specific page"""
        if not self.is_connected():
            return
        
        try:
            patterns = [
                f"page_config:{page_id}",
                f"metrics:{page_id}:*",
                f"rate_limit:{page_id}:*"
            ]
            
            for pattern in patterns:
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
                    
            print(f"[REDIS] Invalidated cache for page {page_id}", file=sys.stderr)
        except Exception as e:
            print(f"[REDIS] Error invalidating cache: {str(e)}", file=sys.stderr)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics"""
        if not self.is_connected():
            return {"connected": False}
        
        try:
            info = self.redis_client.info()
            return {
                "connected": True,
                "used_memory": info.get("used_memory_human", "Unknown"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0)
            }
        except Exception as e:
            print(f"[REDIS] Error getting stats: {str(e)}", file=sys.stderr)
            return {"connected": False, "error": str(e)}
    
    # Pub/Sub for real-time notifications
    def publish_message_event(self, event_type: str, data: Dict[str, Any]):
        """Publish message event for real-time processing"""
        if not self.is_connected():
            return False
        
        try:
            channel = f"chatbot_events:{event_type}"
            message = json.dumps({
                "timestamp": time.time(),
                "event_type": event_type,
                "data": data
            })
            self.redis_client.publish(channel, message)
            return True
        except Exception as e:
            print(f"[REDIS] Error publishing event: {str(e)}", file=sys.stderr)
            return False

# Global cache manager instance
cache_manager = RedisCacheManager()