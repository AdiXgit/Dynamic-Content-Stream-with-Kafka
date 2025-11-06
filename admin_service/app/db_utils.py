# app/db_utils.py

import sqlite3

DB_PATH = "control.db"

def get_connection():
    """Create and return a SQLite connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # enables dict-like row access
    return conn
    
    
    
    
