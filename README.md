# NYC TLC Data Pipeline

A comprehensive data processing and visualization solution for New York City Taxi & Limousine Commission (TLC) trip records using cloud technologies and distributed computing frameworks.

## Features

- Automated data ingestion from NYC TLC website
- Scalable data processing using Apache Spark
- Cloud-native architecture (GCP & Azure support)
- Advanced data warehousing with BigQuery/Synapse
- Real-time monitoring and alerting
- Interactive visualizations with Superset/Metabase

## Architecture

                                 ┌─────────────────┐
                                 │   NYC TLC API   │
                                 └────────┬────────┘
                                         │
                                         ▼
┌─────────────┐              ┌─────────────────────┐
│   Apache    │──────────────▶│    Apache NiFi     │
│   Airflow   │              └──────────┬──────────┘
└─────────────┘                         │
       │                                ▼
       │                     ┌─────────────────────┐
       │                     │   Cloud Storage     │
       │                     └──────────┬──────────┘
       │                                │
       ▼                                ▼
┌─────────────┐              ┌─────────────────────┐
│   Apache    │◀─────────────│    Data Quality     │
│   Spark     │              │      Checks         │
└──────┬──────┘              └─────────────────────┘
       │
       ▼
┌─────────────────────┐      ┌─────────────────────┐
│   BigQuery/Synapse  │─────▶│     Superset/       │
│                     │      │     Metabase         │
└─────────────────────┘      └─────────────────────┘

## Repository Structure

```
nyc_tlc_pipeline/
├── config/                     # Configuration files
│   ├── gcp_config.yaml        # GCP-specific settings
│   ├── azure_config.yaml      # Azure-specific settings
│   ├── airflow_config.yaml    # Airflow configuration
│   └── spark_config.yaml      # Spark configuration
├── dags/                      # Airflow DAG definitions
│   ├── tlc_data_pipeline.py   # Main pipeline DAG
│   └── utils/                 # DAG utilities
├── scripts/                   # Helper scripts
│   ├── setup/                 # Setup scripts
│   └── monitoring/           # Monitoring scripts
├── src/                      # Source code
│   ├── ingestion/           # Data ingestion modules
│   ├── processing/          # Data processing modules
│   ├── quality/             # Data quality modules
│   └── visualization/       # Visualization modules
├── tests/                   # Test suite
│   ├── unit/               # Unit tests
│   └── integration/        # Integration tests
├── docs/                   # Documentation
│   ├── DEPLOYMENT_GUIDE.md
│   └── SCALABILITY_AND_OPTIMIZATION.md
├── .env.example           # Environment variables template
├── requirements.txt       # Python dependencies
├── requirements-dev.txt   # Development dependencies
└── README.md             # Project documentation
```

## Quick Start

### Prerequisites

- Python 3.7+
- Docker & Docker Compose
- Google Cloud SDK or Azure CLI
- Apache Airflow 2.6.3
- Apache Spark 3.5.0

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/nyc-tlc-pipeline.git
   cd nyc-tlc-pipeline
   ```

2. Set up Python environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   .\venv\Scripts\activate  # Windows
   pip install -r requirements.txt
   ```

3. Configure cloud credentials:
   ```bash
   # For GCP
   gcloud auth application-default login
   
   # For Azure
   az login
   ```

4. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your configurations
   ```

5. Start the services:
   ```bash
   docker-compose up -d
   ```

### Running the Pipeline

1. Initialize Airflow:
   ```bash
   airflow db init
   airflow users create --username admin --password admin --firstname Anonymous --lastname Admin --role Admin --email admin@example.com
   ```

2. Start Airflow webserver and scheduler:
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

3. Access the Airflow UI at `http://localhost:8080` and enable the `tlc_data_pipeline` DAG.

## Data Flow

1. **Data Ingestion**
   - Automated download from NYC TLC website
   - Data validation and schema enforcement
   - Storage in Cloud Storage/Data Lake

2. **Data Processing**
   - Spark transformations for data cleaning
   - Aggregations and feature engineering
   - Quality checks and validation

3. **Data Warehousing**
   - Loading into BigQuery/Synapse
   - Partitioning and clustering
   - Performance optimization

4. **Visualization**
   - Interactive dashboards
   - Real-time metrics
   - Custom reports

## Configuration

Detailed configuration guides:
- [GCP Deployment Guide](docs/DEPLOYMENT_GUIDE.md#gcp-deployment)
- [Azure Deployment Guide](docs/DEPLOYMENT_GUIDE.md#azure-deployment)
- [Airflow Configuration](docs/DEPLOYMENT_GUIDE.md#airflow-configuration)
- [Spark Tuning](docs/SCALABILITY_AND_OPTIMIZATION.md)

## Monitoring & Maintenance

- Real-time pipeline monitoring
- Error tracking and alerting
- Resource utilization metrics
- Cost optimization recommendations

## Testing

Run the test suite:
```bash
# Unit tests
pytest tests/unit

# Integration tests
pytest tests/integration
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support, email shalinib0204@gmail.com or create an issue in the GitHub repository.

## Acknowledgments

- NYC Taxi & Limousine Commission for providing the data
- Apache Software Foundation for their excellent tools
- Cloud providers for their services

## Additional Resources

- [NYC TLC Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Google Cloud Documentation](https://cloud.google.com/docs)
- [Azure Documentation](https://docs.microsoft.com/azure/)
