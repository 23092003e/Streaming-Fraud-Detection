from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from spark_utils import (
    calculate_distance,
    extract_time_features,
    compute_amt_vs_category_avg
)
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql import functions as F

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
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
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

# Extract probability as a list and select relevant columns
def vector_to_list(vector):
    return vector.toArray().tolist()
vector_to_list_udf = udf(vector_to_list, ArrayType(DoubleType()))

output_df = predictions.select(
    "trans_date_trans_time",
    "cc_num",
    "amt",
    "merchant",
    "category",
    "is_fraud",
    "prediction",
    vector_to_list_udf("probability").alias("probability"),
)

query = output_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
query.awaitTermination()