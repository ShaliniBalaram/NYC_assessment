# NYC TLC Data Pipeline Deployment Guide

This guide provides detailed instructions for deploying the NYC TLC Data Pipeline on both Google Cloud Platform (GCP) and Microsoft Azure.

## Table of Contents
- [Prerequisites](#prerequisites)
- [GCP Deployment](#gcp-deployment)
- [Azure Deployment](#azure-deployment)
- [Common Configuration](#common-configuration)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Local Development Environment
```bash
# Required Software
- Python 3.7+
- Docker Desktop
- Git
- Terraform (optional, for IaC)

# Cloud CLI Tools
- Google Cloud SDK
- Azure CLI
```

### Access Requirements
```yaml
GCP:
  - Project Owner/Editor role
  - Service Account with necessary permissions
  - Enabled APIs:
    - Cloud Storage
    - BigQuery
    - Dataproc
    - Cloud Run
    - Cloud Monitoring

Azure:
  - Subscription Admin role
  - Service Principal
  - Resource Provider registration:
    - Microsoft.Storage
    - Microsoft.Databricks
    - Microsoft.ContainerRegistry
```

## GCP Deployment

### 1. Initial Setup
```bash
# Install Google Cloud SDK
curl https://sdk.cloud.google.com | bash
gcloud init

# Set project and authenticate
gcloud config set project scenic-flux-441021-t4
gcloud auth application-default login

# Enable required APIs
gcloud services enable storage-api.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable dataproc.googleapis.com
gcloud services enable monitoring.googleapis.com
```

### 2. Infrastructure Setup
```bash
# Create Cloud Storage bucket
gsutil mb -l US gs://nyc_tlc_data_bucket/

# Create BigQuery dataset
bq mk --dataset \
  --description "NYC TLC Data Analytics" \
  --location US \
  scenic-flux-441021-t4:nyc_tlc_data

# Create Dataproc cluster
gcloud dataproc clusters create tlc-processing-cluster \
  --region us-central1 \
  --master-machine-type n1-standard-4 \
  --worker-machine-type n1-standard-4 \
  --num-workers 2 \
  --initialization-actions gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh
```

### 3. Service Configuration
```yaml
# Apache Airflow on Cloud Composer
gcloud composer environments create tlc-airflow \
  --location us-central1 \
  --image-version composer-2.0.0 \
  --python-version 3 \
  --env-variables "BUCKET_NAME=nyc_tlc_data_bucket,PROJECT_ID=scenic-flux-441021-t4"

# Apache NiFi on GKE
gcloud container clusters create tlc-nifi-cluster \
  --num-nodes 3 \
  --machine-type n1-standard-2 \
  --zone us-central1-a

# Deploy visualization tools
gcloud run deploy superset \
  --image apache/superset \
  --platform managed \
  --port 8088

gcloud run deploy metabase \
  --image metabase/metabase \
  --platform managed \
  --port 3000
```

### 4. GCP-Specific Configurations
```python
# Example GCP configuration file: gcp_config.py
GCP_CONFIG = {
    'project_id': 'scenic-flux-441021-t4',
    'bucket_name': 'nyc_tlc_data_bucket',
    'region': 'us-central1',
    'dataset_id': 'nyc_tlc_data',
    'dataproc_cluster': 'tlc-processing-cluster',
    'composer_environment': 'tlc-airflow'
}

# BigQuery-specific settings
BIGQUERY_CONFIG = {
    'location': 'US',
    'partition_field': 'pickup_datetime',
    'clustering_fields': ['pickup_location_id', 'payment_type']
}
```

## Azure Deployment

### 1. Initial Setup
```bash
# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az login

# Set subscription
az account set --subscription <subscription-id>

# Create Resource Group
az group create \
  --name nyc-tlc-rg \
  --location eastus
```

### 2. Infrastructure Setup
```bash
# Create Storage Account
az storage account create \
  --name nyctlcdata \
  --resource-group nyc-tlc-rg \
  --location eastus \
  --sku Standard_LRS

# Create Data Lake container
az storage container create \
  --name raw-data \
  --account-name nyctlcdata

# Create Azure Synapse workspace
az synapse workspace create \
  --name nyc-tlc-synapse \
  --resource-group nyc-tlc-rg \
  --storage-account nyctlcdata \
  --file-system raw-data \
  --sql-admin-login-user sqladmin \
  --sql-admin-login-password <password>
```

### 3. Service Configuration
```yaml
# Azure Databricks
az databricks workspace create \
  --resource-group nyc-tlc-rg \
  --name nyc-tlc-databricks \
  --location eastus \
  --sku standard

# Azure Data Factory
az datafactory create \
  --name nyc-tlc-adf \
  --resource-group nyc-tlc-rg \
  --location eastus

# Deploy visualization tools using Azure Container Instances
az container create \
  --resource-group nyc-tlc-rg \
  --name superset \
  --image apache/superset \
  --dns-name-label nyc-tlc-superset \
  --ports 8088

az container create \
  --resource-group nyc-tlc-rg \
  --name metabase \
  --image metabase/metabase \
  --dns-name-label nyc-tlc-metabase \
  --ports 3000
```

### 4. Azure-Specific Configurations
```python
# Example Azure configuration file: azure_config.py
AZURE_CONFIG = {
    'subscription_id': '<subscription-id>',
    'resource_group': 'nyc-tlc-rg',
    'location': 'eastus',
    'storage_account': 'nyctlcdata',
    'container_name': 'raw-data',
    'synapse_workspace': 'nyc-tlc-synapse',
    'databricks_workspace': 'nyc-tlc-databricks'
}

# Synapse-specific settings
SYNAPSE_CONFIG = {
    'pool_name': 'tlcpool',
    'data_warehouse_units': 100,
    'partition_column': 'pickup_datetime',
    'distribution': 'HASH(pickup_location_id)'
}
```

## Common Configuration

### 1. Environment Variables
```bash
# Create .env file
cat > .env << EOL
# Common Settings
ENVIRONMENT=production
LOG_LEVEL=INFO

# GCP Settings (if using GCP)
GOOGLE_CLOUD_PROJECT=scenic-flux-441021-t4
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# Azure Settings (if using Azure)
AZURE_TENANT_ID=<tenant-id>
AZURE_SUBSCRIPTION_ID=<subscription-id>
AZURE_CLIENT_ID=<client-id>
AZURE_CLIENT_SECRET=<client-secret>

# Apache Airflow
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# Apache NiFi
NIFI_WEB_HTTP_PORT=8080
NIFI_CLUSTER_IS_NODE=true

# Visualization Tools
SUPERSET_PORT=8088
METABASE_PORT=3000
EOL
```

### 2. Update Configuration Files
```python
# Update config.py with platform-specific settings
def get_platform_config():
    if os.getenv('CLOUD_PLATFORM') == 'GCP':
        return GCP_CONFIG
    else:
        return AZURE_CONFIG
```

### 3. Deploy Pipeline Code
```bash
# Clone repository
git clone <repository-url>
cd nyc-tlc-pipeline

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-airflow.txt

# Deploy DAGs
cp dags/* $AIRFLOW_HOME/dags/
```

## Troubleshooting

### Common Issues

1. **Authentication Issues**
```bash
# GCP
gcloud auth application-default login
gcloud auth configure-docker

# Azure
az login
az acr login --name <registry-name>
```

2. **Permission Issues**
```yaml
GCP:
  - Verify IAM roles
  - Check service account permissions
  - Validate API enablement

Azure:
  - Check role assignments
  - Verify service principal permissions
  - Validate resource provider registration
```

3. **Network Issues**
```bash
# Test connectivity
# GCP
gcloud compute networks list
gcloud compute firewall-rules list

# Azure
az network vnet list
az network nsg list
```

### Health Checks

```bash
# Check service status
# GCP
gcloud composer environments describe tlc-airflow --location us-central1
gcloud container clusters get-credentials tlc-nifi-cluster --zone us-central1-a

# Azure
az synapse workspace show --name nyc-tlc-synapse --resource-group nyc-tlc-rg
az databricks workspace show --name nyc-tlc-databricks --resource-group nyc-tlc-rg
```

### Logging and Monitoring

```python
# Example monitoring setup
def setup_monitoring():
    if os.getenv('CLOUD_PLATFORM') == 'GCP':
        setup_cloud_monitoring()
    else:
        setup_azure_monitor()

def setup_cloud_monitoring():
    # GCP Cloud Monitoring setup
    monitoring_client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{GCP_CONFIG['project_id']}"
    
    # Create custom metrics
    descriptor = monitoring_v3.MetricDescriptor(
        type_='custom.googleapis.com/tlc/processing_time',
        metric_kind=monitoring_v3.MetricDescriptor.MetricKind.GAUGE,
        value_type=monitoring_v3.MetricDescriptor.ValueType.DOUBLE,
        description='Data processing time in seconds'
    )
    monitoring_client.create_metric_descriptor(name=project_name, 
                                            metric_descriptor=descriptor)

def setup_azure_monitor():
    # Azure Monitor setup
    credential = DefaultAzureCredential()
    monitor_client = MonitorManagementClient(
        credential,
        AZURE_CONFIG['subscription_id']
    )
    
    # Create custom metrics
    monitor_client.metric_definitions.create_or_update(
        resource_group_name=AZURE_CONFIG['resource_group'],
        metric_name='processing_time',
        metric_properties={
            'displayName': 'Processing Time',
            'unit': 'Seconds'
        }
    )
```

This deployment guide provides comprehensive instructions for setting up the NYC TLC Data Pipeline on both GCP and Azure. Follow the platform-specific sections based on your chosen cloud provider.
