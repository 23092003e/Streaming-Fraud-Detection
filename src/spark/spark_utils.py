from pyspark.sql import functions as F
from pyspark.sql.functions import col, hour, dayofweek, month, year, to_timestamp, radians, sin, cos, sqrt, asin

def calculate_distance(df):
    """Calculate distance between cardholder and merchant locations using Haversine formula."""
    return df.withColumn("lat_r", radians("lat")) \
             .withColumn("lon_r", radians("long")) \
             .withColumn("mlat_r", radians("merch_lat")) \
             .withColumn("mlon_r", radians("merch_long")) \
             .withColumn("dlat", col("lat_r") - col("mlat_r")) \
             .withColumn("dlon", col("lon_r") - col("mlon_r")) \
             .withColumn("a", sin(col("dlat")/2)**2 + cos(col("lat_r"))*cos(col("mlat_r"))*sin(col("dlon")/2)**2) \
             .withColumn("distance", 2 * 6371 * asin(sqrt(col("a")))) \
             .drop("lat_r", "lon_r", "mlat_r", "mlon_r", "dlat", "dlon", "a")

def extract_time_features(df):
    """Extract time-based features from transaction date and dob."""
    return df.withColumn("trans_date", to_timestamp("trans_date_trans_time")) \
             .withColumn("hour", hour("trans_date")) \
             .withColumn("day_of_week", dayofweek("trans_date")) \
             .withColumn("month", month("trans_date")) \
             .withColumn("dob", to_timestamp("dob")) \
             .withColumn("age", (year("trans_date") - year("dob")))

def clean_transaction_inputs(df):
    """Drop invalid records before feature engineering."""
    return df.dropna() \
             .dropDuplicates(["trans_num"]) \
             .filter(col("amt") >= 0) \
             .filter(col("city_pop") >= 0)

def compute_amt_vs_category_avg(df, static_avg, fallback_avg=70.0):
    """Calculate amount relative to average amount by category using a static lookup."""
    result_df = df.join(
        static_avg,
        df.category == static_avg.category,
        "left_outer"
    ) \
    .withColumn("amt_vs_category_avg", col("amt") / F.coalesce(col("avg_amt"), F.lit(float(fallback_avg)))) \
    .drop(static_avg.category, "avg_amt")
    return result_df
