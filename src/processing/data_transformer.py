import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import *
from typing import Union, Dict, List
from google.cloud import bigquery
import os

class DataTransformer:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.PANDAS_THRESHOLD = 1_000_000  # Switch to PySpark if rows > 1M

    def transform_with_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data using Pandas for smaller datasets"""
        
        # Time-based features
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
        
        transformed = df.assign(
            # Date components
            pickup_date=df['pickup_datetime'].dt.date,
            pickup_hour=df['pickup_datetime'].dt.hour,
            pickup_day=df['pickup_datetime'].dt.day,
            pickup_month=df['pickup_datetime'].dt.month,
            pickup_year=df['pickup_datetime'].dt.year,
            pickup_dayofweek=df['pickup_datetime'].dt.dayofweek,
            
            # Time intervals
            trip_duration_minutes=(df['dropoff_datetime'] - df['pickup_datetime'])
                .dt.total_seconds() / 60,
            
            # Derived metrics
            speed_mph=df['trip_distance'] / 
                     ((df['dropoff_datetime'] - df['pickup_datetime'])
                      .dt.total_seconds() / 3600),
            cost_per_mile=df['total_amount'] / df['trip_distance'].replace(0, float('nan')),
            cost_per_minute=df['total_amount'] / 
                          ((df['dropoff_datetime'] - df['pickup_datetime'])
                           .dt.total_seconds() / 60),
            
            # Categories
            trip_distance_category=pd.cut(df['trip_distance'], 
                                        bins=[0, 2, 5, 10, float('inf')],
                                        labels=['short', 'medium', 'long', 'very_long']),
            time_of_day=pd.cut(df['pickup_datetime'].dt.hour,
                             bins=[-1, 6, 10, 15, 19, 23],
                             labels=['early_morning', 'morning_rush', 'midday', 
                                    'evening_rush', 'night'])
        )
        
        # Boolean flags
        transformed['is_weekend'] = transformed['pickup_dayofweek'].isin([5, 6])
        transformed['is_rush_hour'] = transformed['time_of_day'].isin(['morning_rush', 'evening_rush'])
        
        # Payment analysis
        transformed['tip_percentage'] = (df['tip_amount'] / df['fare_amount']) * 100
        
        return transformed

    def transform_with_spark(self, df: SparkDataFrame) -> SparkDataFrame:
        """Transform data using PySpark for larger datasets"""
        
        # Time-based features
        df = df.withColumn('pickup_datetime', F.to_timestamp('pickup_datetime'))
        df = df.withColumn('dropoff_datetime', F.to_timestamp('dropoff_datetime'))
        
        # Extract date components
        df = df.withColumn('pickup_date', F.to_date('pickup_datetime')) \
               .withColumn('pickup_hour', F.hour('pickup_datetime')) \
               .withColumn('pickup_day', F.dayofmonth('pickup_datetime')) \
               .withColumn('pickup_month', F.month('pickup_datetime')) \
               .withColumn('pickup_year', F.year('pickup_datetime')) \
               .withColumn('pickup_dayofweek', F.dayofweek('pickup_datetime'))
        
        # Calculate time intervals
        df = df.withColumn('trip_duration_minutes', 
                          F.round((F.unix_timestamp('dropoff_datetime') - 
                                 F.unix_timestamp('pickup_datetime')) / 60, 2))
        
        # Derived metrics
        df = df.withColumn('speed_mph', 
                          F.round(F.col('trip_distance') / 
                                 (F.col('trip_duration_minutes') / 60), 2)) \
               .withColumn('cost_per_mile',
                          F.round(F.col('total_amount') / 
                                 F.when(F.col('trip_distance') > 0, 
                                      F.col('trip_distance')), 2)) \
               .withColumn('cost_per_minute',
                          F.round(F.col('total_amount') / 
                                 F.col('trip_duration_minutes'), 2))
        
        # Categories
        df = df.withColumn('trip_distance_category',
                          F.when(F.col('trip_distance') <= 2, 'short')
                           .when(F.col('trip_distance') <= 5, 'medium')
                           .when(F.col('trip_distance') <= 10, 'long')
                           .otherwise('very_long'))
        
        df = df.withColumn('time_of_day',
                          F.when(F.col('pickup_hour') <= 6, 'early_morning')
                           .when(F.col('pickup_hour') <= 10, 'morning_rush')
                           .when(F.col('pickup_hour') <= 15, 'midday')
                           .when(F.col('pickup_hour') <= 19, 'evening_rush')
                           .otherwise('night'))
        
        # Boolean flags
        df = df.withColumn('is_weekend', 
                          F.col('pickup_dayofweek').isin([5, 6])) \
               .withColumn('is_rush_hour',
                          F.col('time_of_day').isin(['morning_rush', 'evening_rush']))
        
        # Payment analysis
        df = df.withColumn('tip_percentage',
                          F.round((F.col('tip_amount') / F.col('fare_amount')) * 100, 2))
        
        return df

    def create_analytics_table(self, year: int, month: int) -> None:
        """Create analytics-ready table in BigQuery"""
        table_id = f"{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}"
        
        # Define schema for analytics table
        schema = [
            bigquery.SchemaField("trip_id", "STRING"),
            bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
            bigquery.SchemaField("dropoff_datetime", "TIMESTAMP"),
            bigquery.SchemaField("pickup_date", "DATE"),
            bigquery.SchemaField("pickup_hour", "INTEGER"),
            bigquery.SchemaField("pickup_day", "INTEGER"),
            bigquery.SchemaField("pickup_month", "INTEGER"),
            bigquery.SchemaField("pickup_year", "INTEGER"),
            bigquery.SchemaField("pickup_dayofweek", "INTEGER"),
            bigquery.SchemaField("trip_duration_minutes", "FLOAT"),
            bigquery.SchemaField("trip_distance", "FLOAT"),
            bigquery.SchemaField("speed_mph", "FLOAT"),
            bigquery.SchemaField("passenger_count", "INTEGER"),
            bigquery.SchemaField("fare_amount", "FLOAT"),
            bigquery.SchemaField("tip_amount", "FLOAT"),
            bigquery.SchemaField("total_amount", "FLOAT"),
            bigquery.SchemaField("cost_per_mile", "FLOAT"),
            bigquery.SchemaField("cost_per_minute", "FLOAT"),
            bigquery.SchemaField("tip_percentage", "FLOAT"),
            bigquery.SchemaField("trip_distance_category", "STRING"),
            bigquery.SchemaField("time_of_day", "STRING"),
            bigquery.SchemaField("is_weekend", "BOOLEAN"),
            bigquery.SchemaField("is_rush_hour", "BOOLEAN"),
            bigquery.SchemaField("payment_type", "STRING"),
            bigquery.SchemaField("pickup_location_id", "INTEGER"),
            bigquery.SchemaField("dropoff_location_id", "INTEGER")
        ]
        
        # Create table with time-based partitioning
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="pickup_date"
        )
        
        # Create clustering fields for better query performance
        table.clustering_fields = ["pickup_location_id", "payment_type", "time_of_day"]
        
        # Create the table
        try:
            self.bq_client.delete_table(table_id, not_found_ok=True)
            table = self.bq_client.create_table(table)
            print(f"Created table {table_id}")
        except Exception as e:
            print(f"Error creating table {table_id}: {str(e)}")
            raise

    def transform_and_load(self, data: Union[pd.DataFrame, SparkDataFrame], 
                          year: int, month: int) -> None:
        """Transform data and load into analytics table"""
        try:
            # Determine whether to use Pandas or PySpark based on data size
            if isinstance(data, pd.DataFrame):
                if len(data) > self.PANDAS_THRESHOLD:
                    print("Data size exceeds Pandas threshold, converting to PySpark")
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    data = spark.createDataFrame(data)
                    transformed_data = self.transform_with_spark(data)
                else:
                    transformed_data = self.transform_with_pandas(data)
            else:
                transformed_data = self.transform_with_spark(data)
            
            # Create analytics table
            self.create_analytics_table(year, month)
            
            # Load transformed data
            table_id = f"{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}"
            
            if isinstance(transformed_data, pd.DataFrame):
                # Load using Pandas
                transformed_data.to_gbq(
                    destination_table=f"{self.dataset_id}.tlc_analytics_{year}_{month:02d}",
                    project_id=self.project_id,
                    if_exists='append'
                )
            else:
                # Load using PySpark
                transformed_data.write \
                    .format("bigquery") \
                    .option("table", table_id) \
                    .option("temporaryGcsBucket", "nyc_tlc_data_bucket") \
                    .mode("append") \
                    .save()
            
            print(f"Successfully transformed and loaded data for {year}-{month:02d}")
            
        except Exception as e:
            print(f"Error transforming and loading data: {str(e)}")
            raise
