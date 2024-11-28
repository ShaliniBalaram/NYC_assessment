"""
Enhanced NYC TLC Data Pipeline DAG with robust error handling and monitoring
"""

from datetime import datetime, timedelta
import os
import requests
import logging
from typing import Dict, Any
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage, bigquery
from spark_processor import TLCSparkProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ID = Variable.get('GCP_PROJECT_ID', 'scenic-flux-441021-t4')
BUCKET_NAME = Variable.get('GCS_BUCKET_NAME', 'nyc_tlc_data_bucket')
BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
DATASET_NAME = Variable.get('BQ_DATASET_NAME', 'nyc_tlc_data')
SLACK_WEBHOOK = Variable.get('SLACK_WEBHOOK_URL', '')

# Monitoring thresholds
MIN_RECORDS = 1000
MAX_RECORDS = 10000000
MAX_FILE_AGE_HOURS = 24
MIN_SUCCESS_RATE = 0.95

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
}

def send_slack_notification(context: Dict[str, Any]) -> None:
    """Send notification to Slack"""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    
    if context.get('exception'):
        status = '❌ Failed'
        error_msg = str(context['exception'])
    else:
        status = '✅ Succeeded'
        error_msg = ''

    message = f"""
    *DAG*: {dag_id}
    *Task*: {task_id}
    *Execution Date*: {execution_date}
    *Status*: {status}
    {f'*Error*: {error_msg}' if error_msg else ''}
    """

    if SLACK_WEBHOOK:
        slack_notification = SlackWebhookOperator(
            task_id='slack_notification',
            webhook_token=SLACK_WEBHOOK,
            message=message,
            username='Airflow',
        )
        slack_notification.execute(context)

def validate_source_data(**context) -> str:
    """Validate source data and determine processing path"""
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    filename = f'yellow_tripdata_{year}-{month:02d}.parquet'
    url = f'{BASE_URL}/{filename}'
    
    try:
        # Check if file exists and get metadata
        response = requests.head(url)
        response.raise_for_status()
        
        # Check file age
        last_modified = response.headers.get('last-modified')
        if last_modified:
            last_modified_date = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT')
            age_hours = (datetime.utcnow() - last_modified_date).total_seconds() / 3600
            if age_hours > MAX_FILE_AGE_HOURS:
                logger.warning(f"Source file is older than {MAX_FILE_AGE_HOURS} hours")
                return 'notify_data_issue'
        
        # Check file size
        file_size = int(response.headers.get('content-length', 0))
        if file_size == 0:
            logger.error("Source file is empty")
            return 'notify_data_issue'
        
        return 'download_to_gcs'
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error validating source data: {str(e)}")
        return 'notify_data_issue'

def download_to_gcs(**context) -> str:
    """Download TLC data to GCS with enhanced error handling"""
    try:
        execution_date = context['execution_date']
        year = execution_date.year
        month = execution_date.month
        
        filename = f'yellow_tripdata_{year}-{month:02d}.parquet'
        url = f'{BASE_URL}/{filename}'
        
        # Create temporary directory
        temp_dir = '/tmp/tlc_data'
        os.makedirs(temp_dir, exist_ok=True)
        local_path = os.path.join(temp_dir, filename)
        
        # Download file with progress tracking
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        downloaded_size = 0
        
        with open(local_path, 'wb') as f:
            for data in response.iter_content(block_size):
                f.write(data)
                downloaded_size += len(data)
                progress = (downloaded_size / total_size) * 100
                logger.info(f"Download progress: {progress:.2f}%")
        
        # Upload to GCS with retry logic
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        
        if not bucket.exists():
            bucket = storage_client.create_bucket(BUCKET_NAME, location='US')
        
        gcs_path = f'raw/{year}/{month:02d}/{filename}'
        blob = bucket.blob(gcs_path)
        
        # Upload with retry logic
        for attempt in range(3):
            try:
                blob.upload_from_filename(local_path)
                break
            except Exception as e:
                if attempt == 2:
                    raise
                logger.warning(f"Upload attempt {attempt + 1} failed: {str(e)}")
                continue
        
        # Clean up
        os.remove(local_path)
        
        # Store metrics
        context['task_instance'].xcom_push(
            key='file_metrics',
            value={
                'filename': filename,
                'size_bytes': total_size,
                'gcs_path': f'gs://{BUCKET_NAME}/{gcs_path}'
            }
        )
        
        return f'gs://{BUCKET_NAME}/{gcs_path}'
    
    except Exception as e:
        logger.error(f"Error in download_to_gcs: {str(e)}")
        raise

def process_with_spark(**context) -> None:
    """Process data using Spark with enhanced monitoring"""
    try:
        # Get the GCS path from previous task
        gcs_path = context['task_instance'].xcom_pull(task_ids='download_to_gcs')
        execution_date = context['execution_date']
        
        # Initialize processor
        processor = TLCSparkProcessor(PROJECT_ID, BUCKET_NAME)
        
        # Process and load data with monitoring
        metrics = processor.process_and_load_with_metrics(
            gcs_path,
            DATASET_NAME,
            execution_date.year,
            execution_date.month
        )
        
        # Validate processing results
        if metrics['record_count'] < MIN_RECORDS:
            raise ValueError(f"Record count ({metrics['record_count']}) below minimum threshold ({MIN_RECORDS})")
        
        if metrics['record_count'] > MAX_RECORDS:
            raise ValueError(f"Record count ({metrics['record_count']}) above maximum threshold ({MAX_RECORDS})")
        
        if metrics['success_rate'] < MIN_SUCCESS_RATE:
            raise ValueError(f"Processing success rate ({metrics['success_rate']}) below threshold ({MIN_SUCCESS_RATE})")
        
        # Store metrics
        context['task_instance'].xcom_push(key='processing_metrics', value=metrics)
        
    except Exception as e:
        logger.error(f"Error in process_with_spark: {str(e)}")
        raise

def validate_bigquery_data(**context) -> None:
    """Validate loaded data in BigQuery"""
    try:
        execution_date = context['execution_date']
        year = execution_date.year
        month = execution_date.month
        
        client = bigquery.Client(project=PROJECT_ID)
        
        # Run data quality checks
        query = f"""
        SELECT
            COUNT(*) as record_count,
            COUNT(CASE WHEN pickup_datetime IS NULL THEN 1 END) as null_pickup_count,
            COUNT(CASE WHEN dropoff_datetime IS NULL THEN 1 END) as null_dropoff_count,
            COUNT(CASE WHEN fare_amount < 0 THEN 1 END) as negative_fare_count
        FROM {DATASET_NAME}.tlc_analytics_{year}_{month:02d}
        """
        
        results = client.query(query).result()
        row = next(results)
        
        # Validate results
        if row.null_pickup_count > 0 or row.null_dropoff_count > 0:
            raise ValueError(f"Found {row.null_pickup_count} null pickups and {row.null_dropoff_count} null dropoffs")
        
        if row.negative_fare_count > 0:
            raise ValueError(f"Found {row.negative_fare_count} negative fares")
        
        # Store metrics
        context['task_instance'].xcom_push(
            key='validation_metrics',
            value={
                'record_count': row.record_count,
                'null_pickup_count': row.null_pickup_count,
                'null_dropoff_count': row.null_dropoff_count,
                'negative_fare_count': row.negative_fare_count
            }
        )
        
    except Exception as e:
        logger.error(f"Error in validate_bigquery_data: {str(e)}")
        raise

# Create DAG
dag = DAG(
    'tlc_data_pipeline_enhanced',
    default_args=default_args,
    description='Enhanced NYC TLC Data Pipeline with robust error handling and monitoring',
    schedule_interval='@monthly',
    catchup=False,
    on_failure_callback=send_slack_notification,
    on_success_callback=send_slack_notification,
    tags=['nyc_tlc', 'production']
)

# Define tasks
start = DummyOperator(task_id='start', dag=dag)

validate_source = BranchPythonOperator(
    task_id='validate_source',
    python_callable=validate_source_data,
    provide_context=True,
    dag=dag,
)

notify_data_issue = DummyOperator(
    task_id='notify_data_issue',
    dag=dag,
)

create_dataset = PythonOperator(
    task_id='create_bigquery_dataset',
    python_callable=lambda: bigquery.Client(project=PROJECT_ID).create_dataset(
        f"{PROJECT_ID}.{DATASET_NAME}",
        exists_ok=True
    ),
    dag=dag,
)

download_data = PythonOperator(
    task_id='download_to_gcs',
    python_callable=download_to_gcs,
    provide_context=True,
    dag=dag,
)

check_gcs_file = GCSObjectExistenceSensor(
    task_id='check_gcs_file',
    bucket=BUCKET_NAME,
    object="{{ task_instance.xcom_pull(task_ids='download_to_gcs').split('gs://" + BUCKET_NAME + "/')[1] }}",
    mode='poke',
    poke_interval=60,
    timeout=3600,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_with_spark',
    python_callable=process_with_spark,
    provide_context=True,
    dag=dag,
)

validate_data = PythonOperator(
    task_id='validate_bigquery_data',
    python_callable=validate_bigquery_data,
    provide_context=True,
    dag=dag,
)

check_record_count = BigQueryCheckOperator(
    task_id='check_record_count',
    sql=f"""
    SELECT COUNT(*) >= {MIN_RECORDS}
    FROM {DATASET_NAME}.tlc_analytics_{{{{ execution_date.year }}}}_{{{{ execution_date.strftime('%m') }}}}
    """,
    use_legacy_sql=False,
    dag=dag,
)

end_success = DummyOperator(
    task_id='end_success',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

end_failure = DummyOperator(
    task_id='end_failure',
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# Set task dependencies
start >> validate_source >> [download_data, notify_data_issue]
download_data >> check_gcs_file >> process_data >> validate_data >> check_record_count
[check_record_count, notify_data_issue] >> [end_success, end_failure]
create_dataset >> download_data
