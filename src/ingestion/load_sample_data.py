from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sample_data(n_rows=1000):
    # Generate sample timestamps
    base_date = datetime(2023, 1, 1)
    pickup_times = [base_date + timedelta(hours=np.random.randint(0, 24*30)) 
                   for _ in range(n_rows)]
    dropoff_times = [pt + timedelta(minutes=np.random.randint(5, 120)) 
                    for pt in pickup_times]
    
    # Generate sample locations
    locations = list(range(1, 264))  # NYC TLC location IDs
    
    data = {
        'pickup_datetime': pickup_times,
        'dropoff_datetime': dropoff_times,
        'pickup_location_id': np.random.choice(locations, n_rows),
        'dropoff_location_id': np.random.choice(locations, n_rows),
        'passenger_count': np.random.randint(1, 7, n_rows),
        'trip_distance': np.random.uniform(0.5, 30.0, n_rows),
        'fare_amount': np.random.uniform(2.5, 100.0, n_rows),
        'tip_amount': np.random.uniform(0, 20.0, n_rows),
        'total_amount': None,  # Will calculate
        'payment_type': np.random.choice(['CREDIT', 'CASH', 'MOBILE'], n_rows),
        'trip_type': np.random.choice(['STREET_HAIL', 'DISPATCH'], n_rows)
    }
    
    df = pd.DataFrame(data)
    
    # Calculate derived fields
    df['total_amount'] = df['fare_amount'] + df['tip_amount']
    df['tip_percentage'] = (df['tip_amount'] / df['fare_amount']) * 100
    df['cost_per_mile'] = df['total_amount'] / df['trip_distance']
    df['trip_duration_minutes'] = [(d - p).total_seconds() / 60 for d, p in zip(df['dropoff_datetime'], df['pickup_datetime'])]
    df['speed_mph'] = df['trip_distance'] / (df['trip_duration_minutes'] / 60)
    
    # Time-based features
    df['pickup_hour'] = df['pickup_datetime'].dt.hour
    df['pickup_dayofweek'] = df['pickup_datetime'].dt.day_name()
    df['is_weekend'] = df['pickup_datetime'].dt.dayofweek.isin([5, 6])
    
    # Categorize time of day
    df['time_of_day'] = pd.cut(df['pickup_hour'], 
                              bins=[-1, 6, 12, 18, 24], 
                              labels=['NIGHT', 'MORNING', 'AFTERNOON', 'EVENING'])
    
    # Distance categories
    df['trip_distance_category'] = pd.cut(df['trip_distance'],
                                        bins=[0, 2, 5, 10, float('inf')],
                                        labels=['SHORT', 'MEDIUM', 'LONG', 'VERY_LONG'])
    
    return df

def load_to_bigquery():
    client = bigquery.Client(project="scenic-flux-441021-t4")
    
    # Generate sample data
    df = generate_sample_data()
    
    # Define table schema
    schema = [
        bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
        bigquery.SchemaField("dropoff_datetime", "TIMESTAMP"),
        bigquery.SchemaField("pickup_location_id", "INTEGER"),
        bigquery.SchemaField("dropoff_location_id", "INTEGER"),
        bigquery.SchemaField("passenger_count", "INTEGER"),
        bigquery.SchemaField("trip_distance", "FLOAT"),
        bigquery.SchemaField("fare_amount", "FLOAT"),
        bigquery.SchemaField("tip_amount", "FLOAT"),
        bigquery.SchemaField("total_amount", "FLOAT"),
        bigquery.SchemaField("payment_type", "STRING"),
        bigquery.SchemaField("trip_type", "STRING"),
        bigquery.SchemaField("tip_percentage", "FLOAT"),
        bigquery.SchemaField("cost_per_mile", "FLOAT"),
        bigquery.SchemaField("trip_duration_minutes", "FLOAT"),
        bigquery.SchemaField("speed_mph", "FLOAT"),
        bigquery.SchemaField("pickup_hour", "INTEGER"),
        bigquery.SchemaField("pickup_dayofweek", "STRING"),
        bigquery.SchemaField("is_weekend", "BOOLEAN"),
        bigquery.SchemaField("time_of_day", "STRING"),
        bigquery.SchemaField("trip_distance_category", "STRING")
    ]
    
    # Create table
    table_id = f"{client.project}.nyc_tlc_data.tlc_analytics_2023"
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Created table {table_id}")
    
    # Load data
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )
    
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()
    
    print(f"Loaded {job.output_rows} rows into {table_id}")

if __name__ == "__main__":
    load_to_bigquery()
