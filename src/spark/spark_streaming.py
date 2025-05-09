from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from spark_utils import (
    calculate_distance,
    extract_time_features,
    compute_amt_vs_category_avg
)

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
    .appName("KafkaSparkStreaming") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transaction_data") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Apply feature engineering
json_df = calculate_distance(json_df)
json_df = extract_time_features(json_df)
json_df = compute_amt_vs_category_avg(json_df, spark)  # Pass spark session

# Load the trained model
model_path = "/src/model/gbt_model"
model = PipelineModel.load(model_path)

# Make predictions
predictions = model.transform(json_df)

# Select relevant columns for output
output_df = predictions.select(
    "trans_date_trans_time", "cc_num", "amt", "merchant", "category", "is_fraud", "prediction", "probability"
)

# Write predictions to console
query = output_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()