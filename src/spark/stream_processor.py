from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import logging
from cassandra.cluster import Cluster
from src.cassandra.cassandra_init import CassandraConnection
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, cassandra_hosts=['cassandra']):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.cassandra_conn = CassandraConnection(hosts=cassandra_hosts)
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        """Create Spark session with necessary configurations."""
        return SparkSession.builder \
            .appName("FraudDetectionStreamProcessor") \
            .config("spark.cassandra.connection.host", ",".join(self.cassandra_hosts)) \
            .config("spark.cassandra.connection.keep_alive_ms", "60000") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "100") \
            .getOrCreate()
            
    def process_stream(self, model):
        """Process streaming data with error handling and checkpointing."""
        try:
            # Define schema for incoming data
            schema = StructType([
                StructField("trans_date_trans_time", StringType()),
                StructField("cc_num", LongType()),
                StructField("amt", DoubleType()),
                StructField("merchant", StringType()),
                StructField("category", StringType())
            ])

            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()

            # Parse JSON data
            parsed_df = df.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")

            # Process each batch
            query = parsed_df.writeStream \
                .foreachBatch(lambda df, epoch_id: self._process_batch(df, epoch_id, model)) \
                .option("checkpointLocation", "/tmp/checkpoint") \
                .start()

            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
            raise

    def _process_batch(self, df, epoch_id, model):
        """Process each micro-batch with retry logic."""
        if df.isEmpty():
            return

        try:
            # Apply model predictions
            predictions = model.transform(df)
            
            # Save to Cassandra with batching and retry
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    from src.cassandra.cassandra_init import insert_predictions_batch
                    records_inserted = insert_predictions_batch(
                        self.cassandra_conn,
                        predictions,
                        batch_size=1000
                    )
                    logger.info(f"Successfully processed batch {epoch_id}: {records_inserted} records")
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        logger.error(f"Failed to process batch {epoch_id} after {max_retries} attempts")
                        raise
                    logger.warning(f"Retry {retry_count}/{max_retries} for batch {epoch_id}")
                    time.sleep(2 ** retry_count)  # Exponential backoff

        except Exception as e:
            logger.error(f"Error processing batch {epoch_id}: {e}")
            raise

    def stop(self):
        """Clean up resources."""
        if self.cassandra_conn:
            self.cassandra_conn.close()
        if self.spark:
            self.spark.stop() 