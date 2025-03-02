from kafka import KafkaConsumer
import json
import pandas as pd
import logging
import time

# Set up logging
logging.basicConfig(
    filename='kafka_consumer.csv',
    level=logging.INFO,
    format='%(asctime)s - [Consumer] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    
    
)   

while True:
    try:
        consumer = KafkaConsumer(
            "fraud-Detection",
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset = 'earliest',
            group_id = "fraud-detection-group"
            
        )
        print("[Consumer] Listening for messages ....")
        break
    except Exception as e:
        print(f'[Error] Kafka Consumer failed: {e}!')
        
        time.sleep(2)

batch = []
batch_size = 500

for message in consumer:
    received_data = message.value
    batch.extend(received_data)
    
    if len(batch) >= batch_size:
        print("-" * 80)
        for i in range(batch_size):
            transaction = batch[i]      
            print(f'{transaction['trans_date_trans_time']} |
                    {transaction['cc_num']} |
                    {transaction['merchant']} |
                    {transaction['amt']} |
                    {transaction['gender']} |
                    {transaction['age']} |
                    {transaction['lat']} |
                    {transaction['long']} |
                    {transaction['city_pop']} |
                    {transaction['job']} |
                    {transaction['unix_time']} |
                    {transaction['merch_lat']} |
                    {transaction['merch_long']} |
                    {transaction['is_fraud']}')    
            
        print("-" * 80)
        logging.info(f'Processed batch: {batch[:batch_size]}')
        batch = batch[batch_size:]
        time.sleep(2)