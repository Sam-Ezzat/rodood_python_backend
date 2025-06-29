"""
Database helper functions for connecting to PostgreSQL

This module provides common functionality for connecting to the PostgreSQL database
and performing operations that are used across multiple modules.
"""

import os
import sys
import psycopg2
from psycopg2 import pool

# Global connection pool
connection_pool = None

def get_db_connection():
    """
    Get a connection from the connection pool.
    If the pool doesn't exist, it will be created.
    
    :return: Database connection from the pool
    """
    global connection_pool
    
    try:
        # Initialize the connection pool if it doesn't exist
        if connection_pool is None:
            # Get database connection string from environment variable
            db_url = os.environ.get("DATABASE_URL")
            
            if not db_url:
                print("ERROR: DATABASE_URL environment variable not set", file=sys.stderr)
                return None
            
            # Create a connection pool with enhanced timeout settings
            # Reduced timeouts to prevent authentication expiry
            connection_pool = pool.SimpleConnectionPool(
                1, 8,  # Reduced max connections for better stability
                db_url,
                connect_timeout=15,  # Shorter connection timeout
                keepalives=1,
                keepalives_idle=20,  # Shorter idle time
                keepalives_interval=5,  # More frequent keepalive checks
                keepalives_count=3,  # Fewer retry attempts
                sslmode='require',  # Explicitly require SSL
                application_name='chatbot-python'  # Better connection tracking
            )
            print("Created PostgreSQL connection pool with improved SSL handling", file=sys.stderr)
        
        # Get a connection from the pool
        connection = connection_pool.getconn()
        return connection
    
    except Exception as e:
        print(f"Error creating database connection: {str(e)}", file=sys.stderr)
        return None

def return_db_connection(connection):
    """
    Return a connection to the pool
    
    :param connection: The connection to return to the pool
    """
    global connection_pool
    
    if connection_pool and connection:
        connection_pool.putconn(connection)

def execute_query(query, params=None, fetch=False, fetch_one=False):
    """
    Execute a query on the database
    
    :param query: SQL query to execute
    :param params: Parameters for the query (optional)
    :param fetch: Whether to fetch results (default: False)
    :param fetch_one: Whether to fetch only one result (default: False)
    :return: Query results if fetch is True, otherwise None
    """
    connection = None
    try:
        connection = get_db_connection()
        if not connection:
            return None
        
        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        result = None
        if fetch:
            if fetch_one:
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
        
        connection.commit()
        cursor.close()
        return result
    
    except Exception as e:
        print(f"Error executing query: {str(e)}", file=sys.stderr)
        if connection:
            connection.rollback()
        return None
    
    finally:
        if connection:
            return_db_connection(connection)