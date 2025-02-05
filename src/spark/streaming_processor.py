from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_stream():
    spark = SparkSession.builder \
        .appName("FraudDetection") \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "transactions") \
        .load()

    query = df.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()