# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC # NYC TLC Data Processing Pipeline
# MAGIC This notebook processes NYC Taxi & Limousine Commission (TLC) trip data using Databricks Community Edition.

# COMMAND ----------
# Configuration
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MOUNT_POINT = "/mnt/nyc_tlc"

# COMMAND ----------
# Schema Definition
trip_schema = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("rate_code_id", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pu_location_id", IntegerType(), True),
    StructField("do_location_id", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("extra", FloatType(), True),
    StructField("mta_tax", FloatType(), True),
    StructField("tip_amount", FloatType(), True),
    StructField("tolls_amount", FloatType(), True),
    StructField("improvement_surcharge", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("congestion_surcharge", FloatType(), True)
])

# COMMAND ----------
def load_taxi_zone_lookup():
    """Load and cache taxi zone lookup data"""
    zone_lookup = spark.read.csv(
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv",
        header=True
    )
    return zone_lookup.cache()

# COMMAND ----------
def clean_trip_data(df):
    """Clean and validate trip data"""
    return df.filter(
        # Remove invalid trips
        (F.col("trip_distance") >= 0) &
        (F.col("fare_amount") >= 0) &
        (F.col("total_amount") >= 0) &
        (F.col("passenger_count") > 0) &
        # Remove outliers
        (F.col("trip_distance") < F.expr("percentile_approx(trip_distance, 0.99)")) &
        (F.col("total_amount") < F.expr("percentile_approx(total_amount, 0.99)")) &
        # Ensure valid timestamps
        (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )

# COMMAND ----------
def enrich_trip_data(df, zone_lookup):
    """Enrich trip data with additional features and zone information"""
    # Join with zone lookup for pickup and dropoff locations
    enriched_df = df \
        .join(
            zone_lookup.select(
                F.col("LocationID").alias("pu_location_id"),
                F.col("Borough").alias("pickup_borough"),
                F.col("Zone").alias("pickup_zone")
            ),
            "pu_location_id"
        ) \
        .join(
            zone_lookup.select(
                F.col("LocationID").alias("do_location_id"),
                F.col("Borough").alias("dropoff_borough"),
                F.col("Zone").alias("dropoff_zone")
            ),
            "do_location_id"
        )
    
    # Add time-based features
    enriched_df = enriched_df \
        .withColumn("trip_duration_minutes",
            F.round(
                (F.unix_timestamp("tpep_dropoff_datetime") - 
                 F.unix_timestamp("tpep_pickup_datetime")) / 60,
                2
            )
        ) \
        .withColumn("cost_per_minute",
            F.when(F.col("trip_duration_minutes") > 0,
                  F.round(F.col("total_amount") / F.col("trip_duration_minutes"), 2)
            ).otherwise(None)
        ) \
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
        .withColumn("pickup_day", F.dayofweek("tpep_pickup_datetime")) \
        .withColumn("pickup_month", F.month("tpep_pickup_datetime")) \
        .withColumn("is_weekend", 
            F.when(F.dayofweek("tpep_pickup_datetime").isin([1, 7]), True)
            .otherwise(False)
        ) \
        .withColumn("time_of_day",
            F.when(F.col("pickup_hour").between(6, 10), "Morning Rush")
            .when(F.col("pickup_hour").between(11, 16), "Midday")
            .when(F.col("pickup_hour").between(17, 20), "Evening Rush")
            .otherwise("Night")
        )
    
    return enriched_df

# COMMAND ----------
def calculate_trip_statistics(df):
    """Calculate various trip statistics"""
    # Overall statistics
    overall_stats = df.agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue")
    )
    
    # Borough-level statistics
    borough_stats = df.groupBy("pickup_borough").agg(
        F.count("*").alias("trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue")
    )
    
    # Time-based statistics
    time_stats = df.groupBy("time_of_day").agg(
        F.count("*").alias("trips"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare")
    )
    
    # Popular routes
    popular_routes = df.groupBy(
        "pickup_zone", "dropoff_zone"
    ).agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare")
    ).orderBy(F.desc("trip_count")).limit(10)
    
    return {
        "overall": overall_stats,
        "by_borough": borough_stats,
        "by_time": time_stats,
        "popular_routes": popular_routes
    }

# COMMAND ----------
def process_month_data(year, month):
    """Process one month of TLC data"""
    # Load zone lookup data
    zone_lookup = load_taxi_zone_lookup()
    
    # Load raw trip data
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    raw_df = spark.read.schema(trip_schema).parquet(f"{BASE_URL}/{filename}")
    
    # Clean data
    cleaned_df = clean_trip_data(raw_df)
    
    # Enrich data
    enriched_df = enrich_trip_data(cleaned_df, zone_lookup)
    
    # Calculate statistics
    statistics = calculate_trip_statistics(enriched_df)
    
    # Save processed data
    enriched_df.write.mode("overwrite").parquet(f"{MOUNT_POINT}/processed/{year}/{month:02d}/trips")
    
    # Save statistics
    for stat_name, stat_df in statistics.items():
        stat_df.write.mode("overwrite").parquet(
            f"{MOUNT_POINT}/statistics/{year}/{month:02d}/{stat_name}"
        )
    
    return enriched_df, statistics

# COMMAND ----------
# Example usage
# year, month = 2023, 1
# processed_df, stats = process_month_data(year, month)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Metrics
# MAGIC - Records processed: {processed_df.count()}
# MAGIC - Null values: {processed_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in processed_df.columns])}
# MAGIC - Value ranges: {processed_df.select([F.min(c).alias(f"min_{c}"), F.max(c).alias(f"max_{c}") for c in ["trip_distance", "total_amount", "trip_duration_minutes"]])}
