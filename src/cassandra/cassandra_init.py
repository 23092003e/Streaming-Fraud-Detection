from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, RoundRobinPolicy
from cassandra.pool import HostDistance
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

class CassandraConnection:
    def __init__(self, hosts=['cassandra'], port=9042):
        self.hosts = hosts
        self.port = port
        self.cluster = None
        self.session = None
        self.connect()

    def connect(self):
        """Connect to Cassandra with enhanced configuration."""
        retry_policy = RetryPolicy()
        
        # Enhanced cluster configuration
        self.cluster = Cluster(
            self.hosts,
            port=self.port,
            load_balancing_policy=RoundRobinPolicy(),
            retry_policy=retry_policy,
            protocol_version=4,
            connect_timeout=10
        )
        
        # Configure connection pools
        self.cluster.connection_class.max_requests_per_connection = 32768
        self.cluster.connection_class.min_requests_per_connection = 100
        
        # Set pool settings
        self.cluster.set_connection_class_distance(HostDistance.LOCAL, 2, 100)
        self.cluster.set_connection_class_distance(HostDistance.REMOTE, 1, 10)

        self.session = self.cluster.connect()
        
    def ensure_connection(self):
        """Ensure connection is alive and reconnect if needed."""
        try:
            self.session.execute("SELECT release_version FROM system.local")
        except Exception as e:
            logging.error(f"Connection error: {e}")
            logging.info("Attempting to reconnect...")
            self.connect()
        return self.session

    def close(self):
        """Close connections properly."""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()

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

def insert_predictions_batch(cassandra_conn, predictions_df, batch_size=1000):
    """Insert predictions in batches with connection check."""
    insert_query = """
    INSERT INTO fraud_detection.predictions (
        trans_date_trans_time, cc_num, amt, merchant, category, 
        is_fraud, prediction, probability
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    session = cassandra_conn.ensure_connection()
    
    # Prepare the statement for better performance
    prepared_stmt = session.prepare(insert_query)
    
    # Process in batches
    rows = predictions_df.collect()
    total_count = 0
    batch_count = 0
    
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            for row in batch:
                session.execute(prepared_stmt, (
                    row.trans_date_trans_time,
                    row.cc_num,
                    row.amt,
                    row.merchant,
                    row.category,
                    row.is_fraud,
                    row.prediction,
                    row.probability
                ))
            batch_count += 1
            total_count += len(batch)
            logging.info(f"Inserted batch {batch_count} ({total_count} records total)")
            
    except Exception as e:
        logging.error(f"Error inserting batch: {e}")
        # Ensure connection is still alive
        session = cassandra_conn.ensure_connection()
        raise
        
    return total_count

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