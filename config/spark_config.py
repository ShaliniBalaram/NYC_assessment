from pyspark.sql import SparkSession
import os

def create_spark_session(app_name="NYC_TLC_Processing"):
    """
    Create a Spark session with proper configurations for GCP integration
    """
    # Set up GCP credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'scenic-flux-441021-t4-777aa30cd3f7.json'
    
    # Create Spark session with necessary configurations
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", 
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1,"
                "org.apache.hadoop:hadoop-aws:3.3.2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate()
    
    return spark
