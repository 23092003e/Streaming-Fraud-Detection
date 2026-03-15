import logging
import os

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql import functions as F

from spark_utils import (
    calculate_distance,
    clean_transaction_inputs,
    compute_amt_vs_category_avg,
    extract_time_features,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


SCHEMA = StructType([
    StructField("trans_date_trans_time", StringType()),
    StructField("cc_num", LongType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("amt", DoubleType()),
    StructField("first", StringType()),
    StructField("last", StringType()),
    StructField("gender", StringType()),
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip", IntegerType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("city_pop", IntegerType()),
    StructField("job", StringType()),
    StructField("dob", StringType()),
    StructField("trans_num", StringType()),
    StructField("unix_time", LongType()),
    StructField("merch_lat", DoubleType()),
    StructField("merch_long", DoubleType()),
    StructField("is_fraud", IntegerType()),
])


def get_env(name, default):
    return os.environ.get(name, default)


KAFKA_BOOTSTRAP_SERVERS = get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = get_env("KAFKA_TOPIC", "transaction_data")
MODEL_PATH = get_env("MODEL_PATH", "/src/model/rf_model")
REFERENCE_DATA_PATH = get_env("REFERENCE_DATA_PATH", "/data/raw/fraudTrain.csv")
CHECKPOINT_LOCATION = get_env("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/fraud-stream")
POSTGRES_HOST = get_env("POSTGRES_HOST", "postgres")
POSTGRES_PORT = get_env("POSTGRES_PORT", "5432")
POSTGRES_DB = get_env("POSTGRES_DB", "streaming")
POSTGRES_USER = get_env("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD", "password")
POSTGRES_TABLE = get_env("POSTGRES_TABLE", "fraud_predictions")
PROCESSING_TIME = get_env("PROCESSING_TIME", "10 seconds")


spark = (
    SparkSession.builder
    .appName("FraudDetectionStreamProcessor")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", get_env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
    .getOrCreate()
)
logging.info("Spark session initialized.")


stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)
logging.info("Kafka streaming source initialized.")


json_df = (
    stream_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), SCHEMA).alias("data"))
    .select("data.*")
)
json_df = clean_transaction_inputs(json_df)
logging.info("Input stream cleaned.")


reference_df = spark.read.csv(REFERENCE_DATA_PATH, header=True, schema=SCHEMA)
reference_df = clean_transaction_inputs(reference_df)
static_avg = reference_df.groupBy("category").agg(F.avg("amt").alias("avg_amt")).cache()
fallback_avg = reference_df.agg(F.avg("amt").alias("avg_amt")).first()["avg_amt"] or 70.0
logging.info("Reference category averages cached from %s.", REFERENCE_DATA_PATH)


json_df = calculate_distance(json_df)
json_df = extract_time_features(json_df)
json_df = compute_amt_vs_category_avg(json_df, static_avg, fallback_avg=fallback_avg)
logging.info("Feature engineering applied.")


model = PipelineModel.load(MODEL_PATH)
logging.info("Model loaded from %s.", MODEL_PATH)


predictions = model.transform(json_df)
output_df = predictions.select(
    col("trans_num"),
    col("trans_date_trans_time"),
    col("cc_num"),
    col("amt"),
    col("merchant"),
    col("category"),
    col("is_fraud"),
    col("prediction"),
    vector_to_array(col("probability")).getItem(0).alias("prob_0"),
    vector_to_array(col("probability")).getItem(1).alias("prob_1"),
)
logging.info("Prediction output schema prepared.")


jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def process_batches(batch_df, epoch_id):
    try:
        if batch_df.limit(1).count() == 0:
            logging.info("Batch %s is empty. Skipping write.", epoch_id)
            return

        logging.info("Writing batch %s to PostgreSQL.", epoch_id)
        (
            batch_df.write
            .format("jdbc")
            .mode("append")
            .option("url", jdbc_url)
            .option("dbtable", POSTGRES_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .save()
        )
        logging.info("Batch %s successfully written to PostgreSQL.", epoch_id)
    except Exception as exc:
        logging.exception("Error writing batch %s to PostgreSQL: %s", epoch_id, exc)


query = (
    output_df.writeStream
    .trigger(processingTime=PROCESSING_TIME)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .foreachBatch(process_batches)
    .outputMode("append")
    .start()
)

query.awaitTermination()
