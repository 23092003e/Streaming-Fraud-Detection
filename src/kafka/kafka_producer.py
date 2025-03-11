import os
import time
import pandas as pd
from kafka import KafkaProducer
import json

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "my_topic")

def read_data_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    data = read_data_from_csv('clean_train.csv')
    
    # Giả sử ta sẽ gửi từng dòng lên Kafka, mô phỏng real-time
    for idx, row in data.iterrows():
        message = row.to_dict()  # Chuyển row thành dict
        producer.send(TOPIC_NAME, value=message)
        print(f"Sent: {message}")
        time.sleep(1)  # Mô phỏng chờ 1s rồi gửi tiếp
    
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
