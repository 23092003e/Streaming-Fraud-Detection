from cassandra.cluster import Cluster
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def connect_to_cassandra(host=['cassandra'], max_retries=20, retry_interval=5):
    """Connect to Cassandra with retry logic."""
    logging.info("Connecting to Cassandra...")
    cluster = None
    session = None
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Attempting to connect to Cassandra (attempt {attempt+1}/{max_retries})...")
            cluster = Cluster(host)
            session = cluster.connect(wait_for_all_pools=True)
            logging.info("Successfully connected to Cassandra!")
            return cluster, session
        except Exception as e:
            logging.error(f"Connection attempt failed: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logging.error("Maximum retries reached. Exiting.")
                return None, None

def setup_database(session):
    """Create keyspace and table for storing predictions."""
    # Create keyspace
    keyspace_query = """
    CREATE KEYSPACE IF NOT EXISTS fraud_detection
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    session.execute(keyspace_query)
    logging.info("Keyspace 'fraud_detection' created or already exists.")

    # Create table
    table_query = """
    CREATE TABLE IF NOT EXISTS fraud_detection.predictions (
        trans_date_trans_time text,
        cc_num bigint,
        amt double,
        merchant text,
        category text,
        is_fraud int,
        prediction double,
        probability list<double>,
        PRIMARY KEY (trans_date_trans_time, cc_num)
    )
    """
    session.execute(table_query)
    logging.info("Table 'predictions' created or already exists.")

def insert_predictions(session, predictions_df):
    """Insert prediction values from a Spark DataFrame into the database."""
    insert_query = """
    INSERT INTO fraud_detection.predictions (
        trans_date_trans_time, cc_num, amt, merchant, category, is_fraud, prediction, probability
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Collect DataFrame rows to process them
    rows = predictions_df.collect()
    count = 0
    for row in rows:
        session.execute(insert_query, (
            row.trans_date_trans_time,
            row.cc_num,
            row.amt,
            row.merchant,
            row.category,
            row.is_fraud,
            row.prediction,
            row.probability
        ))
        count += 1
    logging.info(f"Inserted {count} predictions")

def verify_data(session, limit=10):
    """Retrieve and display data from the database."""
    rows = session.execute(f"SELECT trans_date_trans_time, cc_num, amt, merchant, category, is_fraud, prediction, probability FROM fraud_detection.predictions LIMIT {limit}")
    for row in rows:
        print(f"Transaction Time: {row.trans_date_trans_time}, "
              f"CC Num: {row.cc_num}, "
              f"Amount: {row.amt}, "
              f"Merchant: {row.merchant}, "
              f"Category: {row.category}, "
              f"Is Fraud: {row.is_fraud}, "
              f"Prediction: {row.prediction}, "
              f"Probability: {row.probability}")