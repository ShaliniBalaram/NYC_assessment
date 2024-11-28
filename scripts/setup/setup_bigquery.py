from google.cloud import bigquery

def setup_bigquery():
    # Initialize the BigQuery client
    client = bigquery.Client(project="scenic-flux-441021-t4")

    # Create dataset
    dataset_id = f"{client.project}.nyc_tlc_data"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Created dataset {client.project}.nyc_tlc_data")

    # Read and execute SQL views
    with open('dashboard_views.sql', 'r') as f:
        views_sql = f.read()
    
    # Split into individual view statements
    view_statements = views_sql.split(';')
    
    # Execute each view creation statement
    for statement in view_statements:
        if statement.strip():
            try:
                client.query(statement).result()
                print(f"Successfully executed view creation")
            except Exception as e:
                print(f"Error executing statement: {e}")

if __name__ == "__main__":
    setup_bigquery()
