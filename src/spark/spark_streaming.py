from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from spark_utils import (
    calculate_distance,
    extract_time_features,
    compute_amt_vs_category_avg
)
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.functions import vector_to_array

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

schema = StructType([
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
    StructField("is_fraud", IntegerType())
])

spark = SparkSession.builder \
        .appName("FraudDetectionStreamProcessor") \
        .config("spark.streaming.stopGracefullyonShutdown", True)\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
        .config("spark.sql.shuffle.partitions", 4)\
        .master("local[*]")\
        .getOrCreate()
logging.info("Spark session initialized.")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transaction_data") \
    .option("startingOffsets", "earliest") \
    .load()
logging.info("Kafka streaming source initialized.")

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

static_df = spark.read.csv("/data/raw/fraudTest.csv", header=True, inferSchema=True)
static_avg = static_df.groupBy("category").agg(F.avg("amt").alias("avg_amt")).cache()
logging.info("Static category averages cached.")

# Apply feature engineering
json_df = calculate_distance(json_df)
json_df = extract_time_features(json_df)
json_df = compute_amt_vs_category_avg(json_df, static_avg)
logging.info("Feature engineering applied.")

# Load the trained model
model_path = "/src/model/rf_model"
model = PipelineModel.load(model_path)
logging.info(f"Model loaded from {model_path}.")

# Make predictions
predictions = model.transform(json_df)
logging.info("Predictions generated.")

# # Extract probability as a list and select relevant columns
# def vector_to_list(vector):
#     if vector:
#         return vector.toArray().tolist()
#     return None

# vector_to_list_udf = udf(vector_to_list, ArrayType(DoubleType()))

output_df = predictions.select(
    col("trans_date_trans_time"),
    col("cc_num"),
    col("amt"),
    col("merchant"),
    col("category"),
    col("is_fraud"),
    col("prediction"),
    vector_to_array(col("probability")).getItem(0).alias("prob_0"),
    vector_to_array(col("probability")).getItem(1).alias("prob_1")
)
logging.info("Schema of Prediction output:")
logging.info(output_df.schema)

# Define JDBC connection properties
port = 5432
host = "postgres"
# table = "employees"
password = "password"
username = "postgres"
database = "streaming"

jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
connection_properties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver"
}

def process_batches(df, epoch_id):
    try:
        logging.info(f"Processing batch {epoch_id} with {df.count()} records")
        df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", jdbc_url) \
            .option("dbtable", "fraud_predictions") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .save()
        logging.info(f"Batch {epoch_id} successfully written to PostgreSQL")
    except Exception as e:
        logging.error(f"Error writing batch {epoch_id} to PostgreSQL: {str(e)}")
    print("~~~~~~~~~~~~~~~~~~~~~~ data loaded ~~~~~~~~~~~~~~~~~~~~~~")
        

# Define a query to postgre table: employees
query = output_df.writeStream \
            .trigger(processingTime='10 seconds') \
            .foreachBatch(process_batches) \
            .outputMode("append") \
            .start()\
            .awaitTermination()