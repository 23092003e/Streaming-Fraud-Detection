from kafka import KafkaProducer
import pandas as pd
import json
import time
import random

def produce_messages(producer, topic, dataframe):
    time.sleep(5)
    for _, row in dataframe.iterrows():
        message = {
            'transaction_id': int(row['transaction_id']),
            'type': row['type'],
            'amount': float(row['amount']),
            'oldbalanceOrg': float(row['oldbalanceOrg']),
            'newbalanceOrig': float(row['newbalanceOrig']),
            'oldbalanceDest': float(row['oldbalanceDest']),
            'newbalanceDest': float(row['newbalanceDest']),
        }

        producer.send(topic, json.dumps(message).encode('utf-8'))
        time.sleep(5)

if __name__=="__main__":
    # Kafka producer instance
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    kafka_topic = 'financial_transactions'

    # Read data from CSV file
    csv_file_path = '/home/abdou/Desktop/fraud_detection/Fraud.csv'
    sample_size = 6000

    # Use pandas to read a random sample of rows
    df = pd.read_csv(csv_file_path, skiprows=lambda i: i > 0 and random.random() > sample_size / 6362620)
    df['transaction_id'] = range(1, len(df) + 1)

    produce_messages(producer, kafka_topic, df)

    producer.close()