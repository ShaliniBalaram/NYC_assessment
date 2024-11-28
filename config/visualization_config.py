"""Configuration for visualization tools (Superset and Metabase)"""

SUPERSET_CONFIG = {
    'host': 'localhost',
    'port': 8088,
    'username': 'admin',
    'password': 'admin',
    'database_name': 'nyc_tlc_data',
    'sqlalchemy_uri': 'bigquery://scenic-flux-441021-t4'
}

METABASE_CONFIG = {
    'host': 'localhost',
    'port': 3000,
    'username': 'admin@admin.com',
    'password': 'metabasepass',
    'database_name': 'nyc_tlc_data'
}

# BigQuery connection details
BIGQUERY_CONFIG = {
    'project_id': 'scenic-flux-441021-t4',
    'dataset_id': 'nyc_tlc_data',
    'credentials_path': 'scenic-flux-441021-t4-777aa30cd3f7.json'
}

# Dashboard configurations
DASHBOARD_CONFIGS = {
    'trip_analysis': {
        'name': 'NYC TLC Trip Analysis',
        'charts': [
            {
                'name': 'Hourly Trip Distribution',
                'type': 'time_series',
                'query': '''
                    SELECT 
                        EXTRACT(HOUR FROM pickup_datetime) as hour,
                        COUNT(*) as trip_count
                    FROM nyc_tlc_data.tlc_analytics_2023
                    GROUP BY 1
                    ORDER BY 1
                '''
            },
            {
                'name': 'Revenue by Payment Type',
                'type': 'pie',
                'query': '''
                    SELECT 
                        payment_type,
                        SUM(total_amount) as total_revenue
                    FROM nyc_tlc_data.tlc_analytics_2023
                    GROUP BY 1
                '''
            }
        ]
    },
    'financial_metrics': {
        'name': 'Financial Performance Metrics',
        'charts': [
            {
                'name': 'Daily Revenue Trend',
                'type': 'line',
                'query': '''
                    SELECT 
                        DATE(pickup_datetime) as date,
                        SUM(total_amount) as daily_revenue
                    FROM nyc_tlc_data.tlc_analytics_2023
                    GROUP BY 1
                    ORDER BY 1
                '''
            }
        ]
    }
}
