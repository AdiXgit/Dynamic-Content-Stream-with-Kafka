import sqlite3

conn = sqlite3.connect('control.db')
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE,
  status TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS user_subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT,
  topic_name TEXT,
  subscribed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(username, topic_name)
)
""")

conn.commit()
conn.close()

print("DB initialized")



