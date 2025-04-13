from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaFraudDetection") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "fraud-topic") \
    .load()

df.selectExpr("CAST(value AS STRING)").writeStream \
    .format("console") \
    .start() \
    .awaitTermination()
