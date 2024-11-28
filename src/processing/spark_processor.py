from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, unix_timestamp, round, hour, dayofweek, month, year, date_format,
    sum, avg, count, stddev, min, max, percentile_approx, expr, when, lit
)
from pyspark.sql.types import DoubleType
from spark_config import create_spark_session
from data_transformer import DataTransformer

class TLCSparkProcessor:
    def __init__(self, project_id, bucket_name):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.spark = create_spark_session()
        self.transformer = DataTransformer(project_id, "nyc_tlc_data")

    def clean_data(self, df):
        """
        Clean the raw data by removing invalid entries and outliers
        """
        # Remove nulls in critical columns
        cleaned_df = df.dropna(subset=[
            'tpep_pickup_datetime', 
            'tpep_dropoff_datetime',
            'trip_distance',
            'fare_amount',
            'total_amount'
        ])

        # Filter out invalid values
        cleaned_df = cleaned_df.filter(
            (col('trip_distance') > 0) &
            (col('trip_distance') < 100) &  # Remove unrealistic distances
            (col('fare_amount') > 0) &
            (col('total_amount') > 0) &
            (col('passenger_count') > 0) &
            (col('passenger_count') < 10)  # Standard taxis don't carry more than 9 passengers
        )

        # Fix data type issues
        cleaned_df = cleaned_df.withColumn('trip_distance', col('trip_distance').cast(DoubleType())) \
                             .withColumn('fare_amount', col('fare_amount').cast(DoubleType())) \
                             .withColumn('total_amount', col('total_amount').cast(DoubleType()))

        return cleaned_df

    def add_time_features(self, df):
        """
        Add time-based features and metrics
        """
        return df.withColumn('pickup_date', date_format('tpep_pickup_datetime', 'yyyy-MM-dd')) \
                .withColumn('pickup_hour', hour('tpep_pickup_datetime')) \
                .withColumn('pickup_day', dayofweek('tpep_pickup_datetime')) \
                .withColumn('pickup_month', month('tpep_pickup_datetime')) \
                .withColumn('pickup_year', year('tpep_pickup_datetime')) \
                .withColumn('trip_duration_minutes',
                           round((unix_timestamp('tpep_dropoff_datetime') -
                                 unix_timestamp('tpep_pickup_datetime')) / 60, 2)) \
                .withColumn('is_weekend', when(col('pickup_day').isin([1, 7]), 1).otherwise(0)) \
                .withColumn('time_of_day',
                           when(col('pickup_hour').between(6, 10), 'morning_rush')
                           .when(col('pickup_hour').between(11, 16), 'midday')
                           .when(col('pickup_hour').between(17, 20), 'evening_rush')
                           .when(col('pickup_hour').between(21, 23), 'night')
                           .otherwise('late_night'))

    def add_distance_features(self, df):
        """
        Add distance-based features and metrics
        """
        return df.withColumn('speed_mph',
                           when(col('trip_duration_minutes') > 0,
                                round(col('trip_distance') / (col('trip_duration_minutes') / 60), 2))
                           .otherwise(0)) \
                .withColumn('is_long_trip', when(col('trip_distance') > 10, 1).otherwise(0)) \
                .withColumn('distance_category',
                           when(col('trip_distance') <= 2, 'short')
                           .when(col('trip_distance') <= 5, 'medium')
                           .when(col('trip_distance') <= 10, 'long')
                           .otherwise('very_long'))

    def add_payment_features(self, df):
        """
        Add payment and cost-related features
        """
        return df.withColumn('cost_per_mile',
                           when(col('trip_distance') > 0,
                                round(col('total_amount') / col('trip_distance'), 2))
                           .otherwise(0)) \
                .withColumn('cost_per_minute',
                           when(col('trip_duration_minutes') > 0,
                                round(col('total_amount') / col('trip_duration_minutes'), 2))
                           .otherwise(0)) \
                .withColumn('tip_percentage',
                           when(col('fare_amount') > 0,
                                round(col('tip_amount') / col('fare_amount') * 100, 2))
                           .otherwise(0)) \
                .withColumn('payment_type_desc',
                           when(col('payment_type') == 1, 'credit_card')
                           .when(col('payment_type') == 2, 'cash')
                           .when(col('payment_type') == 3, 'no_charge')
                           .when(col('payment_type') == 4, 'dispute')
                           .otherwise('unknown'))

    def calculate_trip_metrics(self, df, year, month):
        """
        Calculate various trip metrics and statistics
        """
        # Daily aggregations
        daily_metrics = df.groupBy('pickup_date') \
            .agg(
                count('*').alias('total_trips'),
                sum('total_amount').alias('total_revenue'),
                avg('trip_distance').alias('avg_distance'),
                avg('trip_duration_minutes').alias('avg_duration'),
                avg('tip_percentage').alias('avg_tip_percentage')
            )

        # Hourly aggregations
        hourly_metrics = df.groupBy('pickup_date', 'pickup_hour') \
            .agg(
                count('*').alias('trips_per_hour'),
                avg('total_amount').alias('avg_fare_per_hour')
            )

        # Location-based metrics
        location_metrics = df.groupBy('pu_location_id') \
            .agg(
                count('*').alias('pickups_count'),
                avg('total_amount').alias('avg_fare'),
                avg('trip_distance').alias('avg_trip_distance')
            )

        # Payment type distribution
        payment_metrics = df.groupBy('payment_type_desc') \
            .agg(
                count('*').alias('payment_count'),
                sum('total_amount').alias('total_revenue'),
                avg('tip_percentage').alias('avg_tip_percentage')
            )

        # Write metrics to BigQuery
        self.write_metrics_to_bigquery(daily_metrics, 'daily_metrics', year, month)
        self.write_metrics_to_bigquery(hourly_metrics, 'hourly_metrics', year, month)
        self.write_metrics_to_bigquery(location_metrics, 'location_metrics', year, month)
        self.write_metrics_to_bigquery(payment_metrics, 'payment_metrics', year, month)

    def write_metrics_to_bigquery(self, df, metric_type, year, month):
        """
        Write metrics to separate BigQuery tables
        """
        table_id = f"{self.project_id}.{metric_type}.{year}_{month:02d}"
        
        df.write \
            .format('bigquery') \
            .option('table', table_id) \
            .option('temporaryGcsBucket', self.bucket_name) \
            .mode('overwrite') \
            .save()

    def process_data(self, input_path, year, month):
        """
        Main data processing pipeline
        """
        # Read data
        df = self.spark.read.parquet(input_path)

        # Clean data
        cleaned_df = self.clean_data(df)

        # Add features
        processed_df = cleaned_df
        processed_df = self.add_time_features(processed_df)
        processed_df = self.add_distance_features(processed_df)
        processed_df = self.add_payment_features(processed_df)

        # Calculate and save metrics
        self.calculate_trip_metrics(processed_df, year, month)

        return processed_df

    def write_to_bigquery(self, df, dataset_name, year, month):
        """
        Write processed data to BigQuery with partitioning
        """
        table_id = f"{self.project_id}.{dataset_name}.trips_{year}_{month:02d}"
        
        df.write \
            .format('bigquery') \
            .option('table', table_id) \
            .option('temporaryGcsBucket', self.bucket_name) \
            .option('partitionField', 'pickup_date') \
            .option('partitionType', 'DAY') \
            .option('clusteredFields', 'pickup_hour,payment_type_desc') \
            .mode('overwrite') \
            .save()

    def process_and_load(self, input_path, dataset_name, year, month):
        """
        Complete pipeline: process data and load to BigQuery
        """
        try:
            # Process data
            processed_df = self.process_data(input_path, year, month)
            
            # Write main processed data
            self.write_to_bigquery(processed_df, dataset_name, year, month)
            
            return True
        except Exception as e:
            print(f"Error processing data: {str(e)}")
            return False
        finally:
            self.spark.stop()

    def process_monthly_data(self, year: int, month: int) -> None:
        """Process a month of TLC data"""
        try:
            # Load and process data
            input_path = f"gs://{self.bucket_name}/tlc_data/{year}/{month:02d}/*.parquet"
            df = self.spark.read.parquet(input_path)
            
            # Transform and load to analytics table
            self.transformer.transform_and_load(df, year, month)
            
            # Create visualization views
            from bigquery_views import BigQueryViewManager
            view_manager = BigQueryViewManager(self.project_id)
            view_manager.create_datasets()
            view_manager.create_visualization_views(year, month)
            
            print(f"Successfully processed data for {year}-{month:02d}")
        except Exception as e:
            print(f"Error processing data for {year}-{month:02d}: {str(e)}")
            raise
