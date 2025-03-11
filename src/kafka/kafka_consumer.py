import os
from kafka import KafkaConsumer
import json

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "my_topic")

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
    )
    print("Consumer started, waiting for messages...")
    for message in consumer:
        data = message.value
        print(f"Received: {data}")

if __name__ == "__main__":
    main()
