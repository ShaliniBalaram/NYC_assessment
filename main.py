import os
import logging
from datetime import datetime
import load_sample_data
import setup_bigquery
import setup_dashboards
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import subprocess
import time
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NYCTLCAnalytics:
    def __init__(self):
        self.project_id = "scenic-flux-441021-t4"
        self.client = bigquery.Client(project=self.project_id)
        self.figures_dir = "figures"
        self.airflow_home = os.getenv('AIRFLOW_HOME', os.path.join(os.getcwd(), 'airflow'))
        self.nifi_api_port = 9090
        self.spark_master = "local[*]"
        os.makedirs(self.figures_dir, exist_ok=True)

    def setup_data(self):
        """Initialize sample data and BigQuery setup"""
        try:
            logger.info("Starting data setup...")
            
            # Load sample data
            load_sample_data.load_to_bigquery()
            logger.info("Sample data loaded successfully")
            
            # Setup BigQuery views
            setup_bigquery.setup_bigquery()
            logger.info("BigQuery views created successfully")
            
        except Exception as e:
            logger.error(f"Error in data setup: {str(e)}")
            raise

    def setup_airflow(self):
        """Initialize and configure Airflow"""
        try:
            logger.info("Setting up Airflow...")
            
            # Set Airflow environment variables
            os.environ['AIRFLOW_HOME'] = self.airflow_home
            
            # Initialize Airflow database
            subprocess.run(['airflow', 'db', 'init'], check=True)
            
            # Start Airflow scheduler in background
            subprocess.Popen(['airflow', 'scheduler'], 
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
            
            # Start Airflow webserver in background
            subprocess.Popen(['airflow', 'webserver', '--port', '8080'],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
            
            logger.info("Airflow setup completed")
            
        except Exception as e:
            logger.error(f"Error setting up Airflow: {str(e)}")
            raise

    def setup_nifi(self):
        """Start and configure NiFi"""
        try:
            logger.info("Setting up NiFi...")
            
            # Start NiFi service
            subprocess.Popen(['nifi.bat', 'run'], 
                           cwd=os.environ.get('NIFI_HOME'),
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
            
            # Wait for NiFi to start
            time.sleep(60)  # Give NiFi time to initialize
            
            logger.info("NiFi setup completed")
            
        except Exception as e:
            logger.error(f"Error setting up NiFi: {str(e)}")
            raise

    def setup_spark(self):
        """Initialize Spark session"""
        try:
            logger.info("Setting up Spark...")
            
            # Initialize Spark session
            self.spark = SparkSession.builder \
                .appName("NYC_TLC_Analysis") \
                .master(self.spark_master) \
                .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
                .getOrCreate()
            
            logger.info("Spark setup completed")
            
        except Exception as e:
            logger.error(f"Error setting up Spark: {str(e)}")
            raise

    def trigger_airflow_dag(self):
        """Trigger the Airflow DAG for data processing"""
        try:
            logger.info("Triggering Airflow DAG...")
            
            # Trigger the DAG
            subprocess.run(['airflow', 'dags', 'trigger', 'tlc_data_pipeline'],
                         check=True)
            
            logger.info("Airflow DAG triggered successfully")
            
        except Exception as e:
            logger.error(f"Error triggering Airflow DAG: {str(e)}")
            raise

    def process_with_spark(self):
        """Process data using Spark"""
        try:
            logger.info("Processing data with Spark...")
            
            # Read data from BigQuery
            df = self.spark.read \
                .format("bigquery") \
                .option("table", f"{self.project_id}:nyc_tlc_data.tlc_analytics_2023") \
                .load()
            
            # Perform Spark transformations
            df.createOrReplaceTempView("tlc_data")
            
            # Example Spark SQL transformation
            result = self.spark.sql("""
                SELECT 
                    pickup_location_id,
                    COUNT(*) as trip_count,
                    AVG(fare_amount) as avg_fare
                FROM tlc_data
                GROUP BY pickup_location_id
                ORDER BY trip_count DESC
            """)
            
            # Save results back to BigQuery
            result.write \
                .format("bigquery") \
                .option("table", f"{self.project_id}:nyc_tlc_data.spark_aggregated_metrics") \
                .mode("overwrite") \
                .save()
            
            logger.info("Spark processing completed")
            
        except Exception as e:
            logger.error(f"Error in Spark processing: {str(e)}")
            raise

    def generate_trip_volume_figures(self):
        """Generate trip volume analysis figures"""
        try:
            logger.info("Generating trip volume figures...")
            
            query = """
            SELECT * FROM `scenic-flux-441021-t4.nyc_tlc_data.dashboard_trip_volume`
            """
            df = self.client.query(query).to_dataframe()
            
            # Hourly patterns
            fig = px.line(df, x='pickup_hour', y='trip_count',
                         title='Hourly Trip Distribution',
                         labels={'pickup_hour': 'Hour of Day',
                                'trip_count': 'Number of Trips'})
            fig.write_html(f"{self.figures_dir}/hourly_patterns.html")
            fig.write_image(f"{self.figures_dir}/hourly_patterns.png")
            
            # Daily patterns
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Bar(x=df['pickup_dayofweek'], y=df['trip_count'],
                      name="Trip Count"),
                secondary_y=False
            )
            fig.add_trace(
                go.Scatter(x=df['pickup_dayofweek'], y=df['avg_fare'],
                          name="Average Fare", mode='lines+markers'),
                secondary_y=True
            )
            fig.update_layout(title='Daily Trip Patterns and Average Fares')
            fig.write_html(f"{self.figures_dir}/daily_patterns.html")
            fig.write_image(f"{self.figures_dir}/daily_patterns.png")
            
            logger.info("Trip volume figures generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating trip volume figures: {str(e)}")
            raise

    def generate_financial_figures(self):
        """Generate financial analysis figures"""
        try:
            logger.info("Generating financial figures...")
            
            query = """
            SELECT * FROM `scenic-flux-441021-t4.nyc_tlc_data.dashboard_financial_metrics`
            """
            df = self.client.query(query).to_dataframe()
            
            # Payment distribution
            fig = px.pie(df, values='trip_count', names='payment_type',
                        title='Trip Distribution by Payment Type')
            fig.write_html(f"{self.figures_dir}/payment_distribution.html")
            fig.write_image(f"{self.figures_dir}/payment_distribution.png")
            
            # Tip patterns
            fig = px.violin(df, x='time_of_day', y='avg_tip_percentage',
                          title='Tip Percentage Distribution by Time of Day',
                          labels={'time_of_day': 'Time of Day',
                                 'avg_tip_percentage': 'Average Tip (%)'})
            fig.write_html(f"{self.figures_dir}/tip_patterns.html")
            fig.write_image(f"{self.figures_dir}/tip_patterns.png")
            
            logger.info("Financial figures generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating financial figures: {str(e)}")
            raise

    def generate_geographic_figures(self):
        """Generate geographic analysis figures"""
        try:
            logger.info("Generating geographic figures...")
            
            query = """
            SELECT * FROM `scenic-flux-441021-t4.nyc_tlc_data.dashboard_geographic_metrics`
            """
            df = self.client.query(query).to_dataframe()
            
            # Top locations
            top_locations = df.groupby('pickup_location_id').agg({
                'trip_count': 'sum',
                'avg_fare': 'mean'
            }).sort_values('trip_count', ascending=False).head(10)
            
            fig = px.bar(top_locations, x=top_locations.index, y='trip_count',
                        title='Top 10 Pickup Locations',
                        labels={'index': 'Location ID',
                               'trip_count': 'Number of Trips'})
            fig.write_html(f"{self.figures_dir}/top_locations.html")
            fig.write_image(f"{self.figures_dir}/top_locations.png")
            
            # Speed vs Distance
            fig = px.scatter(df, x='avg_distance', y='avg_speed',
                           color='avg_fare',
                           title='Speed vs Distance by Location',
                           labels={'avg_distance': 'Average Distance (miles)',
                                  'avg_speed': 'Average Speed (mph)',
                                  'avg_fare': 'Average Fare ($)'})
            fig.write_html(f"{self.figures_dir}/speed_distance.html")
            fig.write_image(f"{self.figures_dir}/speed_distance.png")
            
            logger.info("Geographic figures generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating geographic figures: {str(e)}")
            raise

    def generate_performance_figures(self):
        """Generate performance analysis figures"""
        try:
            logger.info("Generating performance figures...")
            
            query = """
            SELECT * FROM `scenic-flux-441021-t4.nyc_tlc_data.dashboard_performance_metrics`
            """
            df = self.client.query(query).to_dataframe()
            
            # Speed distribution
            fig = px.box(df, x='time_of_day', y='avg_speed',
                        color='is_weekend',
                        title='Speed Distribution by Time',
                        labels={'time_of_day': 'Time of Day',
                               'avg_speed': 'Average Speed (mph)',
                               'is_weekend': 'Weekend'})
            fig.write_html(f"{self.figures_dir}/speed_distribution.html")
            fig.write_image(f"{self.figures_dir}/speed_distribution.png")
            
            # Duration patterns
            fig = px.violin(df, x='trip_distance_category', y='avg_duration',
                          color='is_weekend',
                          title='Trip Duration by Distance Category',
                          labels={'trip_distance_category': 'Distance Category',
                                 'avg_duration': 'Average Duration (minutes)',
                                 'is_weekend': 'Weekend'})
            fig.write_html(f"{self.figures_dir}/duration_patterns.html")
            fig.write_image(f"{self.figures_dir}/duration_patterns.png")
            
            logger.info("Performance figures generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating performance figures: {str(e)}")
            raise

    def generate_statistical_figures(self):
        """Generate statistical analysis figures using Matplotlib/Seaborn"""
        try:
            logger.info("Generating statistical analysis figures...")
            
            query = """
            SELECT 
                pickup_hour,
                AVG(fare_amount) as avg_fare,
                STDDEV(fare_amount) as std_fare,
                COUNT(*) as trip_count
            FROM `scenic-flux-441021-t4.nyc_tlc_data.tlc_analytics_2023`
            GROUP BY pickup_hour
            ORDER BY pickup_hour
            """
            df = self.client.query(query).to_dataframe()
            
            # Set style for all plots
            sns.set_style("whitegrid")
            plt.rcParams['figure.figsize'] = [12, 6]
            
            # Fare distribution by hour
            plt.figure()
            sns.boxplot(data=df, x='pickup_hour', y='avg_fare')
            plt.title('Fare Distribution by Hour')
            plt.xlabel('Hour of Day')
            plt.ylabel('Average Fare ($)')
            plt.savefig(f"{self.figures_dir}/fare_distribution.png")
            plt.close()
            
            # Trip count distribution
            plt.figure()
            sns.barplot(data=df, x='pickup_hour', y='trip_count')
            plt.title('Trip Count Distribution')
            plt.xlabel('Hour of Day')
            plt.ylabel('Number of Trips')
            plt.savefig(f"{self.figures_dir}/trip_distribution.png")
            plt.close()
            
            # Fare variation analysis
            plt.figure()
            fig, ax1 = plt.subplots()
            
            ax1.set_xlabel('Hour of Day')
            ax1.set_ylabel('Average Fare ($)', color='tab:blue')
            ax1.plot(df['pickup_hour'], df['avg_fare'], color='tab:blue')
            ax1.tick_params(axis='y', labelcolor='tab:blue')
            
            ax2 = ax1.twinx()
            ax2.set_ylabel('Standard Deviation', color='tab:orange')
            ax2.plot(df['pickup_hour'], df['std_fare'], color='tab:orange')
            ax2.tick_params(axis='y', labelcolor='tab:orange')
            
            plt.title('Fare Variation Analysis')
            plt.savefig(f"{self.figures_dir}/fare_variation.png")
            plt.close()
            
            logger.info("Statistical analysis figures generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating statistical figures: {str(e)}")
            raise

    def create_index_html(self):
        """Create index.html for easy figure navigation"""
        try:
            logger.info("Creating index.html...")
            
            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>NYC TLC Data Analysis Figures</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    .section { margin-bottom: 30px; }
                    h1, h2 { color: #333; }
                    .figure-grid {
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                        gap: 20px;
                        margin-top: 20px;
                    }
                    .figure-item {
                        border: 1px solid #ddd;
                        padding: 10px;
                        border-radius: 5px;
                    }
                    .figure-item img {
                        max-width: 100%;
                        height: auto;
                    }
                    a { color: #0066cc; text-decoration: none; }
                    a:hover { text-decoration: underline; }
                </style>
            </head>
            <body>
                <h1>NYC TLC Data Analysis Figures</h1>
                
                <div class="section">
                    <h2>Trip Volume Analysis</h2>
                    <div class="figure-grid">
                        <div class="figure-item">
                            <h3>Hourly Patterns</h3>
                            <img src="hourly_patterns.png" alt="Hourly Patterns">
                            <p><a href="hourly_patterns.html">View Interactive Version</a></p>
                        </div>
                        <div class="figure-item">
                            <h3>Daily Patterns</h3>
                            <img src="daily_patterns.png" alt="Daily Patterns">
                            <p><a href="daily_patterns.html">View Interactive Version</a></p>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>Financial Analysis</h2>
                    <div class="figure-grid">
                        <div class="figure-item">
                            <h3>Payment Distribution</h3>
                            <img src="payment_distribution.png" alt="Payment Distribution">
                            <p><a href="payment_distribution.html">View Interactive Version</a></p>
                        </div>
                        <div class="figure-item">
                            <h3>Tip Patterns</h3>
                            <img src="tip_patterns.png" alt="Tip Patterns">
                            <p><a href="tip_patterns.html">View Interactive Version</a></p>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>Geographic Analysis</h2>
                    <div class="figure-grid">
                        <div class="figure-item">
                            <h3>Top Locations</h3>
                            <img src="top_locations.png" alt="Top Locations">
                            <p><a href="top_locations.html">View Interactive Version</a></p>
                        </div>
                        <div class="figure-item">
                            <h3>Speed vs Distance</h3>
                            <img src="speed_distance.png" alt="Speed vs Distance">
                            <p><a href="speed_distance.html">View Interactive Version</a></p>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>Performance Analysis</h2>
                    <div class="figure-grid">
                        <div class="figure-item">
                            <h3>Speed Distribution</h3>
                            <img src="speed_distribution.png" alt="Speed Distribution">
                            <p><a href="speed_distribution.html">View Interactive Version</a></p>
                        </div>
                        <div class="figure-item">
                            <h3>Duration Patterns</h3>
                            <img src="duration_patterns.png" alt="Duration Patterns">
                            <p><a href="duration_patterns.html">View Interactive Version</a></p>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>Statistical Analysis</h2>
                    <div class="figure-grid">
                        <div class="figure-item">
                            <h3>Fare Distribution</h3>
                            <img src="fare_distribution.png" alt="Fare Distribution">
                        </div>
                        <div class="figure-item">
                            <h3>Trip Count Distribution</h3>
                            <img src="trip_distribution.png" alt="Trip Count Distribution">
                        </div>
                        <div class="figure-item">
                            <h3>Fare Variation Analysis</h3>
                            <img src="fare_variation.png" alt="Fare Variation Analysis">
                        </div>
                    </div>
                </div>
            </body>
            </html>
            """
            
            with open(f"{self.figures_dir}/index.html", 'w') as f:
                f.write(html_content)
            
            logger.info("Index.html created successfully")
            
        except Exception as e:
            logger.error(f"Error creating index.html: {str(e)}")
            raise

    def run_analysis(self):
        """Run the complete analysis pipeline"""
        try:
            logger.info("Starting NYC TLC data analysis pipeline...")
            
            # Setup components
            self.setup_airflow()
            self.setup_nifi()
            self.setup_spark()
            
            # Setup data and BigQuery
            self.setup_data()
            
            # Trigger Airflow DAG
            self.trigger_airflow_dag()
            
            # Process with Spark
            self.process_with_spark()
            
            # Generate visualizations
            self.generate_trip_volume_figures()
            self.generate_financial_figures()
            self.generate_geographic_figures()
            self.generate_performance_figures()
            self.generate_statistical_figures()
            
            # Create index.html
            self.create_index_html()
            
            logger.info("Analysis pipeline complete!")
            logger.info(f"Results available in {self.figures_dir}/")
            logger.info("Access Airflow UI at http://localhost:8080")
            logger.info("Access NiFi UI at http://localhost:8080/nifi")
            
        except Exception as e:
            logger.error(f"Error in analysis pipeline: {str(e)}")
            raise

if __name__ == "__main__":
    analytics = NYCTLCAnalytics()
    analytics.run_analysis()
