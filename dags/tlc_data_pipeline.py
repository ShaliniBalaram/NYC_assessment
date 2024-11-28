from datetime import datetime, timedelta
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage, bigquery
from spark_processor import TLCSparkProcessor

# Constants
PROJECT_ID = 'scenic-flux-441021-t4'
BUCKET_NAME = 'nyc_tlc_data_bucket'
BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
DATASET_NAME = 'nyc_tlc_data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_to_gcs(**context):
    """Download TLC data to GCS"""
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    filename = f'yellow_tripdata_{year}-{month:02d}.parquet'
    url = f'{BASE_URL}/{filename}'
    
    # Create temporary directory
    temp_dir = '/tmp/tlc_data'
    os.makedirs(temp_dir, exist_ok=True)
    local_path = os.path.join(temp_dir, filename)
    
    # Download file
    response = requests.get(url)
    with open(local_path, 'wb') as f:
        f.write(response.content)
    
    # Upload to GCS
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    if not bucket.exists():
        bucket = storage_client.create_bucket(BUCKET_NAME, location='US')
    
    blob = bucket.blob(f'raw/{year}/{month:02d}/{filename}')
    blob.upload_from_filename(local_path)
    
    # Clean up
    os.remove(local_path)
    
    return f'gs://{BUCKET_NAME}/raw/{year}/{month:02d}/{filename}'

def process_with_spark(**context):
    """Process data using Spark and load to BigQuery"""
    # Get the GCS path from previous task
    gcs_path = context['task_instance'].xcom_pull(task_ids='download_to_gcs')
    execution_date = context['execution_date']
    
    # Initialize processor
    processor = TLCSparkProcessor(PROJECT_ID, BUCKET_NAME)
    
    # Process and load data
    success = processor.process_and_load(
        gcs_path,
        DATASET_NAME,
        execution_date.year,
        execution_date.month
    )
    
    if not success:
        raise Exception("Spark processing failed")

def create_bigquery_dataset():
    """Create BigQuery dataset if it doesn't exist"""
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = f"{PROJECT_ID}.{DATASET_NAME}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)

# Create DAG
dag = DAG(
    'tlc_data_pipeline',
    default_args=default_args,
    description='NYC TLC Data Pipeline with Enhanced Spark Processing',
    schedule_interval='@monthly',
    catchup=False
)

# Define tasks
create_dataset_task = PythonOperator(
    task_id='create_bigquery_dataset',
    python_callable=create_bigquery_dataset,
    dag=dag,
)

download_task = PythonOperator(
    task_id='download_to_gcs',
    python_callable=download_to_gcs,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_with_spark',
    python_callable=process_with_spark,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
create_dataset_task >> download_task >> process_task
