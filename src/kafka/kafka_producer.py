import os
import time
import csv
import json
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "fraud-transactions")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        api_version=(2, 8, 1)
    )

def simulate_transactions():
    producer = create_producer()
    
    with open(r'D:\Data Science\Big Data Technology\Project\Streaming-Fraud-Detection\Streaming-Fraud-Detection\data\raw\fraudTrain.csv', 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        
        for row in reader:
            transaction = {
                'trans_date_trans_time': row[0],
                'cc_num': row[1],
                'merchant': row[2],
                'category': row[3],
                'amt': float(row[4]),
                'first': row[5],
                'last': row[6],
                'gender': row[7],
                'city': row[9],
                'state': row[10],
                'is_fraud': int(row[-1])
            }
            
            producer.send(TOPIC_NAME, value=transaction)
            print(f"Sent transaction: {transaction['cc_num']}")
            time.sleep(0.5)

if __name__ == "__main__":
    simulate_transactions()