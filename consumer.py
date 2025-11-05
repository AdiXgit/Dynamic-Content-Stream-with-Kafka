import sqlite3, time, threading, json
from kafka import KafkaConsumer

DB='control.db'
KAFKA_BOOTSTRAP='localhost:9092'

def db_query(q, args=(), one=False):
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute(q, args)
    rows = cur.fetchall()
    con.commit()
    con.close()
    return (rows[0] if rows else None) if one else rows

class DynamicConsumer(threading.Thread):
    def __init__(self, group_id):
        super().__init__()
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id=self.group_id,
            enable_auto_commit=True,
            auto_offset_reset='earliest'
        )
        self.current_topics = set()

    def run(self):
        while True:
            rows = db_query("SELECT name FROM topics WHERE status='active'")
            active = set(r[0] for r in rows)
            if active != self.current_topics:
                try:
                    if active:
                        self.consumer.subscribe(list(active))
                        print(f"Subscribed to topics: {active}")
                    else:
                        self.consumer.unsubscribe()
                        print("Unsubscribed from all topics")
                    self.current_topics = active
                except Exception as e:
                    print(f"Subscription error: {e}")
            for msg_batch in self.consumer.poll(timeout_ms=1000, max_records=10).values():
                for record in msg_batch:
                    print(f"[{record.topic}] {record.value}")
            time.sleep(1)

if __name__ == '__main__':
    DynamicConsumer(group_id='dyn-consumer-1').start()

