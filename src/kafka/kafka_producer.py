from kafka import KafkaProducer
import json
import random
import pandas as pd
import threading
import time

class DataGenerator:
    index = 0
    def __init__(self):
        # read from csv
        self.df = pd.read_csv("fraudTest.csv", index_col=0)
        print(self.df.columns)
        
    def generateTransactions(self):
        num = random.randint(1, 10)
        # num = 1
        messages = self.df[self.index: self.index + num].to_dict(orient='records')
        self.index += num
        return messages


class MyProducer:
    topic_name = 'transaction_data'
    
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            client_id="test-producer",
            acks=1,
            retries=5, 
            key_serializer=lambda a:json.dumps(a).encode('utf-8'),
            value_serializer=lambda b:json.dumps(b).encode('utf-8')
        )
    def produce_message(self, message):
        self.producer.send(self.topic_name, message)

    def send_data(self, messages, multi=True):
        # Use threading for concurrent message production
        if multi:
            threads = []
            for message in messages:
                thread = threading.Thread(target=self.produce_message, args=(message,))
                threads.append(thread)
                thread.start()

            # Wait for all threads to finish
            for thread in threads:
                thread.join()

        else:
            future=self.producer.send(self.topic_name, value=message, key=message['txn_id'])
            self.producer.flush()

    def __del__(self):
        self.producer.close()

print("Starting app...")
dataGenerator=DataGenerator()
MyProducer=MyProducer()

try:
    while True:
        temp_data=dataGenerator.generateTransactions()
        MyProducer.send_data(temp_data)
        print(f'Sent {len(temp_data)} messages...')
        time.sleep(3)
except KeyboardInterrupt:
    exit()