# Create scripts directory if it doesn't exist
New-Item -ItemType Directory -Path "scripts/setup" -Force
New-Item -ItemType Directory -Path "scripts/monitoring" -Force

# Move ingestion-related files
Move-Item -Path "nifi_controller.py" -Destination "src/ingestion/" -Force
Move-Item -Path "load_sample_data.py" -Destination "src/ingestion/" -Force

# Move processing-related files
Move-Item -Path "data_transformer.py" -Destination "src/processing/" -Force
Move-Item -Path "spark_processor.py" -Destination "src/processing/" -Force
Move-Item -Path "analysis_queries.py" -Destination "src/processing/" -Force

# Move quality-related files
Move-Item -Path "eda_analysis.py" -Destination "src/quality/" -Force

# Move visualization-related files
Move-Item -Path "setup_visualizations.py" -Destination "src/visualization/" -Force
Move-Item -Path "metabase_integration.py" -Destination "src/visualization/" -Force
Move-Item -Path "superset_integration.py" -Destination "src/visualization/" -Force
Move-Item -Path "setup_dashboards.py" -Destination "src/visualization/" -Force

# Move setup scripts
Move-Item -Path "setup_bigquery.py" -Destination "scripts/setup/" -Force
Move-Item -Path "setup_pipeline.py" -Destination "scripts/setup/" -Force
Move-Item -Path "init_airflow.bat" -Destination "scripts/setup/" -Force
Move-Item -Path "start_pipeline.py" -Destination "scripts/setup/" -Force

# Move configuration files
Move-Item -Path "spark_config.py" -Destination "config/" -Force
Move-Item -Path "superset_config.py" -Destination "config/" -Force
Move-Item -Path "visualization_config.py" -Destination "config/" -Force

# Create .env.example file
@"
# Cloud Provider Settings
CLOUD_PLATFORM=GCP  # or AZURE
PROJECT_ID=scenic-flux-441021-t4
REGION=us-central1

# GCP Settings
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
BUCKET_NAME=nyc_tlc_data_bucket
DATASET_ID=nyc_tlc_data

# Azure Settings
AZURE_TENANT_ID=your_tenant_id
AZURE_SUBSCRIPTION_ID=your_subscription_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret

# Apache Airflow
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# Apache NiFi
NIFI_WEB_HTTP_PORT=8080

# Visualization Tools
SUPERSET_PORT=8088
METABASE_PORT=3000

# Monitoring
SLACK_WEBHOOK_URL=your_slack_webhook_url
ALERT_EMAIL=your_email@example.com
"@ | Out-File -FilePath ".env.example" -Encoding UTF8

# Create requirements-dev.txt
@"
pytest==7.4.3
pytest-cov==4.1.0
black==23.11.0
flake8==6.1.0
mypy==1.7.1
isort==5.12.0
pre-commit==3.5.0
pytest-mock==3.12.0
pytest-asyncio==0.21.1
"@ | Out-File -FilePath "requirements-dev.txt" -Encoding UTF8

Write-Host "Files organized successfully!"
