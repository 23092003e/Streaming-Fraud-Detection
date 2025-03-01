from kafka import KafkaProducer
import pandas as pd
import json
import datetime
import csv
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092', # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize the message
    acks='all' # Wait for all replicas to acknowledge
)


data_path = r"D:\Data Science\Big Data Technology\Project\Streaming-Fraud-Detection\Streaming-Fraud-Detection\data\processed\clean_train.csv"

# Load the data
batch = []
batch_size = 500

try:
    with open(data_path, mode ='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row['Send_Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            batch.append(row)
            
            if len(batch) >= batch_size:
                producer.send('fraud_detection', value=batch)
                print(f"[Producer] Sent batch of {batch_size} messages.")
                batch = []
                time.sleep(0.5)
                
    if batch:
        producer.send('fraud_detection', value=batch)
        print(f"[Producer] Sent final batch of {len(batch)} messages")
        
except Exception as e:
    print(f"[Producer] An error occurred: {e}")
    
finally:
    producer.flush()
    producer.close()