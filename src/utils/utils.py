import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

def create_spark_session():
    """
    Create a Spark session with specific configurations.
    """
    spark = SparkSession.builder \
        .appName("DataProcessing") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    return spark


