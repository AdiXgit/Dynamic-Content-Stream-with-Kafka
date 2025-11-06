# app/models.py

from app.db_utils import get_connection

def create_table():
    """Create the topics table if it doesn't exist."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def add_topic(name, description):
    """Insert a new topic into the database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO topics (name, description) VALUES (?, ?)", (name, description))
    conn.commit()
    conn.close()

def get_all_topics():
    """Return a list of all topics."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM topics ORDER BY created_at DESC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def delete_topic(topic_id):
    """Delete a topic by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM topics WHERE id = ?", (topic_id,))
    conn.commit()
    conn.close()
    
    
    
