import os
import json
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "fraud-transactions")

def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(2, 8, 1)
    )

def consume_transactions():
    consumer = create_consumer()
    print("Starting consumer...")
    
    for message in consumer:
        transaction = message.value
        print(f"""
        Received transaction:
        CC: {transaction['cc_num']}
        Amount: ${transaction['amt']:.2f}
        Merchant: {transaction['merchant']}
        Fraud: {'⚠️' if transaction['is_fraud'] else '✅'}
        """)

if __name__ == "__main__":
    consume_transactions()