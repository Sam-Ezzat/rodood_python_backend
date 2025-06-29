"""
Professional Data Access Layer with Redis Integration
Handles all database operations with proper relationships and caching
"""

import asyncio
import time
import json
import sys
from typing import Dict, Any, Optional, List, Tuple
from contextlib import asynccontextmanager
from redis_cache_manager import cache_manager
import psycopg2
from psycopg2.extras import RealDictCursor
import os

class ProfessionalDataLayer:
    def __init__(self):
        self.db_pool = None
        self.initialize_connection_pool()
    
    def initialize_connection_pool(self):
        """Initialize PostgreSQL connection pool"""
        try:
            import psycopg2.pool
            self.db_pool = psycopg2.pool.ThreadedConnectionPool(
                1, 20,  # min and max connections
                dsn=os.environ['DATABASE_URL'],
                cursor_factory=RealDictCursor
            )
            print("[DATA] Database connection pool initialized", file=sys.stderr)
        except Exception as e:
            print(f"[DATA] Failed to initialize connection pool: {str(e)}", file=sys.stderr)

    @asynccontextmanager
    async def get_db_connection(self):
        """Get database connection from pool"""
        conn = None
        try:
            conn = self.db_pool.getconn()
            yield conn
        except Exception as e:
            print(f"[DATA] Database connection error: {str(e)}", file=sys.stderr)
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.db_pool.putconn(conn)

    # ===== CONVERSATION MANAGEMENT =====
    
    async def get_or_create_conversation(self, page_id: str, sender_id: str, sender_name: str = None) -> Dict[str, Any]:
        """Get existing conversation or create new one"""
        # Check Redis cache first
        conversation_key = f"{page_id}:{sender_id}"
        cached_conv = cache_manager.get_conversation_state(conversation_key)
        if cached_conv:
            return cached_conv
        
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get page internal ID
            cursor.execute("SELECT id FROM pages WHERE page_id = %s", (page_id,))
            page_record = cursor.fetchone()
            if not page_record:
                raise ValueError(f"Page {page_id} not found")
            
            page_internal_id = page_record['id']
            
            # Try to get existing conversation
            cursor.execute("""
                SELECT c.*, p.page_name, p.platform_id
                FROM conversations c
                JOIN pages p ON c.page_id = p.id
                WHERE c.page_id = %s AND c.sender_id = %s AND c.status = 'active'
            """, (page_internal_id, sender_id))
            
            conversation = cursor.fetchone()
            
            if not conversation:
                # Create new conversation
                cursor.execute("""
                    INSERT INTO conversations (page_id, sender_id, sender_name, status, created_at)
                    VALUES (%s, %s, %s, 'active', NOW())
                    RETURNING id, page_id, sender_id, sender_name, status, created_at
                """, (page_internal_id, sender_id, sender_name))
                
                conversation = cursor.fetchone()
                conn.commit()
            
            # Convert to dict and cache
            conv_dict = dict(conversation)
            conv_dict['page_id'] = page_id  # Use external page_id
            
            cache_manager.cache_conversation_state(conversation_key, conv_dict, ttl=3600)
            return conv_dict

    async def get_conversation_messages(self, conversation_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent messages for a conversation"""
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, message_id, sender_type, content, message_type, 
                       attachments, sent_at, response_time_ms
                FROM messages 
                WHERE conversation_id = %s 
                ORDER BY sent_at DESC 
                LIMIT %s
            """, (conversation_id, limit))
            
            messages = cursor.fetchall()
            return [dict(msg) for msg in messages]

    # ===== PAGE CONFIGURATION MANAGEMENT =====
    
    async def get_page_configuration(self, page_id: str) -> Dict[str, Any]:
        """Get complete page configuration with AI assistant details"""
        # Check Redis cache first
        cached_config = cache_manager.get_page_config(page_id)
        if cached_config:
            return cached_config
        
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    p.id, p.page_id, p.page_name, p.access_token, p.status,
                    pc.greeting_message, pc.first_message, pc.end_message, 
                    pc.stop_message, pc.max_messages, pc.response_delay_seconds,
                    pc.business_hours, pc.auto_responses,
                    a.assistant_id, a.name as assistant_name, a.model, a.instructions
                FROM pages p
                LEFT JOIN page_configs pc ON p.id = pc.page_id
                LEFT JOIN assistants a ON pc.assistant_id = a.id
                WHERE p.page_id = %s AND p.status = 'active'
            """, (page_id,))
            
            config = cursor.fetchone()
            if not config:
                return None
            
            config_dict = dict(config)
            
            # Cache the configuration
            cache_manager.cache_page_config(page_id, config_dict, ttl=1800)
            return config_dict

    async def get_platform_mapping(self, instagram_id: str) -> Optional[str]:
        """Get Facebook page ID from Instagram ID"""
        # Check Redis cache first
        cached_mapping = cache_manager.get_platform_mapping(instagram_id)
        if cached_mapping:
            return cached_mapping
        
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT tp.page_id as facebook_page_id
                FROM platform_mappings pm
                JOIN pages sp ON pm.source_page_id = sp.id
                JOIN pages tp ON pm.target_page_id = tp.id
                WHERE sp.page_id = %s AND pm.mapping_type = 'instagram_to_facebook'
                AND pm.is_active = true
            """, (instagram_id,))
            
            mapping = cursor.fetchone()
            if mapping:
                facebook_page_id = mapping['facebook_page_id']
                cache_manager.cache_platform_mapping(instagram_id, facebook_page_id, ttl=86400)
                return facebook_page_id
            
            return None

    # ===== MESSAGE PROCESSING =====
    
    async def save_message(self, conversation_id: int, sender_type: str, content: str, 
                          message_type: str = 'text', attachments: List = None, 
                          response_time_ms: int = None) -> Dict[str, Any]:
        """Save a message to the database"""
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO messages (conversation_id, sender_type, content, message_type, 
                                    attachments, response_time_ms, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                RETURNING id, conversation_id, sender_type, content, sent_at
            """, (conversation_id, sender_type, content, message_type, 
                  json.dumps(attachments or []), response_time_ms))
            
            message = cursor.fetchone()
            conn.commit()
            
            # Update conversation timestamp
            cursor.execute("""
                UPDATE conversations 
                SET updated_at = NOW() 
                WHERE id = %s
            """, (conversation_id,))
            conn.commit()
            
            return dict(message)

    # ===== AI SESSION MANAGEMENT =====
    
    async def get_or_create_ai_session(self, conversation_id: int, assistant_id: str) -> Dict[str, Any]:
        """Get existing AI session or create new one"""
        # Check Redis cache first
        cached_session = cache_manager.get_ai_session(str(conversation_id))
        if cached_session:
            return cached_session
        
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get assistant internal ID
            cursor.execute("SELECT id FROM assistants WHERE assistant_id = %s", (assistant_id,))
            assistant_record = cursor.fetchone()
            if not assistant_record:
                # Create assistant record if doesn't exist
                cursor.execute("""
                    INSERT INTO assistants (assistant_id, name, model, is_active)
                    VALUES (%s, %s, 'gpt-4', true)
                    RETURNING id
                """, (assistant_id, f"Assistant {assistant_id}"))
                assistant_record = cursor.fetchone()
                conn.commit()
            
            assistant_internal_id = assistant_record['id']
            
            # Try to get existing session
            cursor.execute("""
                SELECT id, conversation_id, thread_id, run_id, context_messages, 
                       session_state, token_usage, started_at
                FROM ai_sessions 
                WHERE conversation_id = %s
            """, (conversation_id,))
            
            session = cursor.fetchone()
            
            if not session:
                # Create new session
                cursor.execute("""
                    INSERT INTO ai_sessions (conversation_id, assistant_id, context_messages, 
                                           session_state, started_at)
                    VALUES (%s, %s, '[]', '{}', NOW())
                    RETURNING id, conversation_id, thread_id, run_id, context_messages, 
                             session_state, token_usage, started_at
                """, (conversation_id, assistant_internal_id))
                
                session = cursor.fetchone()
                conn.commit()
            
            session_dict = dict(session)
            
            # Cache the session
            cache_manager.cache_ai_session(str(conversation_id), session_dict, ttl=7200)
            return session_dict

    async def update_ai_session(self, conversation_id: int, thread_id: str = None, 
                               run_id: str = None, context_messages: List = None, 
                               token_usage: int = None) -> None:
        """Update AI session with new data"""
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            update_fields = []
            update_values = []
            
            if thread_id:
                update_fields.append("thread_id = %s")
                update_values.append(thread_id)
            
            if run_id:
                update_fields.append("run_id = %s")
                update_values.append(run_id)
            
            if context_messages is not None:
                update_fields.append("context_messages = %s")
                update_values.append(json.dumps(context_messages))
            
            if token_usage is not None:
                update_fields.append("token_usage = %s")
                update_values.append(token_usage)
            
            if update_fields:
                update_fields.append("updated_at = NOW()")
                update_values.append(conversation_id)
                
                query = f"""
                    UPDATE ai_sessions 
                    SET {', '.join(update_fields)}
                    WHERE conversation_id = %s
                """
                
                cursor.execute(query, update_values)
                conn.commit()
                
                # Invalidate cache
                cache_manager.redis_client.delete(f"ai_session:{conversation_id}")

    # ===== ANALYTICS AND METRICS =====
    
    async def save_conversation_analytics(self, conversation_id: int, sentiment_score: float, 
                                        sentiment_label: str, engagement_score: int = None) -> None:
        """Save conversation analytics"""
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO conversation_analytics (conversation_id, sentiment_score, 
                                                  sentiment_label, engagement_score, analyzed_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (conversation_id) 
                DO UPDATE SET 
                    sentiment_score = EXCLUDED.sentiment_score,
                    sentiment_label = EXCLUDED.sentiment_label,
                    engagement_score = EXCLUDED.engagement_score,
                    analyzed_at = EXCLUDED.analyzed_at
            """, (conversation_id, sentiment_score, sentiment_label, engagement_score))
            
            conn.commit()

    async def get_daily_metrics(self, page_id: str, date: str) -> Dict[str, Any]:
        """Get daily metrics for a page"""
        # Check Redis cache first
        cached_metrics = cache_manager.get_daily_metrics(page_id, date)
        if cached_metrics:
            return cached_metrics
        
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT total_conversations, total_messages, avg_response_time_ms,
                       avg_sentiment_score, user_satisfaction_avg, bot_handoff_rate
                FROM metrics_daily md
                JOIN pages p ON md.page_id = p.id
                WHERE p.page_id = %s AND md.metric_date = %s
            """, (page_id, date))
            
            metrics = cursor.fetchone()
            if metrics:
                metrics_dict = dict(metrics)
                cache_manager.cache_daily_metrics(page_id, date, metrics_dict, ttl=3600)
                return metrics_dict
            
            return {}

    # ===== RATE LIMITING =====
    
    async def check_message_rate_limit(self, sender_id: str, page_id: str) -> bool:
        """Check if sender is within rate limits"""
        return cache_manager.check_rate_limit(sender_id, page_id, limit=10, window=60)

    # ===== SYSTEM LOGGING =====
    
    async def log_system_event(self, level: str, component: str, message: str, 
                              context: Dict = None, user_id: int = None, 
                              page_id: str = None, conversation_id: int = None) -> None:
        """Log system events for monitoring"""
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get page internal ID if provided
            page_internal_id = None
            if page_id:
                cursor.execute("SELECT id FROM pages WHERE page_id = %s", (page_id,))
                page_record = cursor.fetchone()
                if page_record:
                    page_internal_id = page_record['id']
            
            cursor.execute("""
                INSERT INTO system_logs (log_level, component, message, context, 
                                       user_id, page_id, conversation_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """, (level, component, message, json.dumps(context or {}), 
                  user_id, page_internal_id, conversation_id))
            
            conn.commit()

    # ===== WEBHOOK EVENT TRACKING =====
    
    async def save_webhook_event(self, platform_id: int, event_type: str, 
                                raw_payload: Dict) -> int:
        """Save webhook event for processing"""
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO webhook_events (platform_id, event_type, raw_payload, 
                                          processing_status, received_at)
                VALUES (%s, %s, %s, 'pending', NOW())
                RETURNING id
            """, (platform_id, event_type, json.dumps(raw_payload)))
            
            event = cursor.fetchone()
            conn.commit()
            
            return event['id']

    async def mark_webhook_processed(self, event_id: int, success: bool = True, 
                                   error_message: str = None) -> None:
        """Mark webhook event as processed"""
        async with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            status = 'completed' if success else 'failed'
            
            cursor.execute("""
                UPDATE webhook_events 
                SET processed = true, processing_status = %s, 
                    error_message = %s, processed_at = NOW()
                WHERE id = %s
            """, (status, error_message, event_id))
            
            conn.commit()

# Global data layer instance
data_layer = ProfessionalDataLayer()