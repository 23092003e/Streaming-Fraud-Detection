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
        test_data_path = r'./data/raw/fraudTest.csv'
        self.df = pd.read_csv(test_data_path, index_col=0)
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
            boostrap_servers = ['localhost:9092'],
            client_id = 'my-producer',
            acks = 1,
            retries = 5,
            key_serializer = lambda x: json.dumps(x).encode('utf-8'),
            value_serializer = lambda y: json.dumps(y).encode('utf-8')
        )
    
    def produce_message(self, message):
        # send the message to the topic
        self.producer.send(self.topic_name, key=message['transactionId'], value=message)
        print(f"Produced message: {message}")
        
    def send_data(self, message, multi = True):
        if multi:
            threads = []
            for msg in message:
                thread = threading.Thread(target=self.produce_message, args=(msg,))
                threads.append(thread)
                thread.start()
            for thread in threads:
                thread.join()
                
        else:   
            future = self.producer.send(self.topic_name, key=message['transactionId'], value=message)
            