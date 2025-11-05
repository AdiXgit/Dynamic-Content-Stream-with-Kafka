from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import sqlite3, time, threading, json
from flask import Flask, request, jsonify

DB="../admin/control.db"
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

def db(q, args=(), one=False):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute(q, args)
    rows = cur.fetchall()
    conn.commit()
    conn.close()
    return (rows[0] if rows else None) if one else rows

def topic_watcher():
    while True:
        rows = db("SELECT name FROM topics WHERE status='approved'")
        for r in rows:
            topic = r[0]
            try:
                admin.create_topics([NewTopic(topic, 1, 1)])
            except Exception:
                pass
            db("UPDATE topics SET status='active' WHERE name=?", (topic,))
        time.sleep(2)

app = Flask(__name__)

@app.route("/enqueue", methods=["POST"])
def enqueue():
    data = request.json
    producer.send(data["topic"], data["value"])
    producer.flush()
    return jsonify({"sent": True})

threading.Thread(target=topic_watcher, daemon=True).start()

if __name__ == "__main__":
    app.run(port=5001)
