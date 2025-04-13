from kafka import KafkaProducer
import json
import pandas as pd
import time
import os

class DataGenerator:
    def __init__(self):
        test_path = os.path.join("data", "raw", "fraudTest.csv")
        self.df = pd.read_csv(test_path)
        self.df.drop(columns=self.df.columns[0], inplace=True) 

    def get_data(self):
        for _, row in self.df.iterrows():
            yield row.to_dict()

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=json_serializer
)

if __name__ == "__main__":
    print("Producer started...")
    generator = DataGenerator()

    for data in generator.get_data():
        producer.send("fraud-topic", data)
        print("Sent:", data)
        time.sleep(1) 
