from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

file_path = r'D:\Data Science\Big Data Technology\Project\Streaming-Fraud-Detection\Streaming-Fraud-Detection\data\processed\clean_train.csv'
df = pd.read_csv(file_path)

for _, row in df.iterrows():
    message = row.to_dict()
    
    producer.send('fraud-detection', value=message)
    print(f"Sent: {message}")
    
    time.sleep(1)

producer.flush()
producer.close()