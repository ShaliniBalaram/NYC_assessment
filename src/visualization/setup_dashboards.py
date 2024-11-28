from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

class DashboardGenerator:
    def __init__(self, project_id: str = "scenic-flux-441021-t4"):
        self.client = bigquery.Client(project=project_id)
        self.output_dir = "dashboard_outputs"
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_trip_volume_dashboard(self):
        """Generate trip volume visualizations"""
        query = """
        SELECT * FROM `scenic-flux-441021-t4.nyc_tlc_data.dashboard_trip_volume`
        """
        df = self.client.query(query).to_dataframe()

        # Hourly patterns
        fig = px.line(df, x='pickup_hour', y='trip_count', 
                     title='Hourly Trip Patterns')
        fig.write_html(f"{self.output_dir}/hourly_patterns.html")

        # Daily patterns with average fare
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(x=df['pickup_dayofweek'], y=df['trip_count'], name="Trip Count"),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(x=df['pickup_dayofweek'], y=df['avg_fare'], name="Average Fare", mode='lines+markers'),
            secondary_y=True,
        )
        fig.update_layout(title='Daily Patterns: Trip Count and Average Fare')
        fig.write_html(f"{self.output_dir}/daily_patterns.html")

    def generate_financial_dashboard(self):
        """Generate financial metrics visualizations"""
        query = """
        SELECT * FROM `scenic-flux-441021-t4.nyc_tlc_data.dashboard_financial_metrics`
        """
        df = self.client.query(query).to_dataframe()

        # Payment type distribution
        fig = px.pie(df, values='trip_count', names='payment_type',
                    title='Trip Distribution by Payment Type')
        fig.write_html(f"{self.output_dir}/payment_distribution.html")

        # Average fare by time of day
        fig = px.box(df, x='time_of_day', y='avg_fare',
                    title='Fare Distribution by Time of Day')
        fig.write_html(f"{self.output_dir}/fare_by_time.html")

        # Tip percentage patterns
        fig = px.violin(df, x='time_of_day', y='avg_tip_percentage',
                       title='Tip Percentage Distribution by Time of Day')
        fig.write_html(f"{self.output_dir}/tip_patterns.html")

    def generate_geographic_dashboard(self):
        """Generate geographic distribution visualizations"""
        query = """
        SELECT * FROM `scenic-flux-441021-t4.nyc_tlc_data.dashboard_geographic_metrics`
        """
        df = self.client.query(query).to_dataframe()

        # Top pickup locations
        top_locations = df.groupby('pickup_location_id').agg({
            'trip_count': 'sum',
            'avg_fare': 'mean'
        }).sort_values('trip_count', ascending=False).head(10)

        fig = px.bar(top_locations, x=top_locations.index, y='trip_count',
                    title='Top 10 Pickup Locations')
        fig.write_html(f"{self.output_dir}/top_pickups.html")

        # Average speed by location
        fig = px.scatter(df, x='avg_distance', y='avg_speed',
                        color='avg_fare',
                        title='Speed vs Distance by Location',
                        labels={'avg_distance': 'Average Distance (miles)',
                               'avg_speed': 'Average Speed (mph)',
                               'avg_fare': 'Average Fare ($)'})
        fig.write_html(f"{self.output_dir}/speed_patterns.html")

    def generate_performance_dashboard(self):
        """Generate performance metrics visualizations"""
        query = """
        SELECT * FROM `scenic-flux-441021-t4.nyc_tlc_data.dashboard_performance_metrics`
        """
        df = self.client.query(query).to_dataframe()

        # Trip distance categories
        pivot_df = df.pivot_table(index='trip_distance_category', 
                                columns='time_of_day', 
                                values='trip_count', 
                                aggfunc='sum').fillna(0)
        
        fig = px.bar(pivot_df, 
                    title='Trip Counts by Distance Category and Time of Day')
        fig.write_html(f"{self.output_dir}/distance_categories.html")

        # Speed patterns
        fig = px.box(df, x='time_of_day', y='avg_speed',
                    color='is_weekend', 
                    title='Speed Distribution by Time',
                    labels={'time_of_day': 'Time of Day',
                           'avg_speed': 'Average Speed (mph)',
                           'is_weekend': 'Weekend'})
        fig.write_html(f"{self.output_dir}/speed_distribution.html")

        # Duration patterns
        fig = px.violin(df, x='trip_distance_category', y='avg_duration',
                       color='is_weekend', 
                       title='Trip Duration by Distance Category',
                       labels={'trip_distance_category': 'Distance Category',
                              'avg_duration': 'Average Duration (minutes)',
                              'is_weekend': 'Weekend'})
        fig.write_html(f"{self.output_dir}/duration_patterns.html")

    def generate_all_dashboards(self):
        """Generate all dashboard visualizations"""
        print("Generating Trip Volume Dashboard...")
        self.generate_trip_volume_dashboard()

        print("Generating Financial Dashboard...")
        self.generate_financial_dashboard()

        print("Generating Geographic Dashboard...")
        self.generate_geographic_dashboard()

        print("Generating Performance Dashboard...")
        self.generate_performance_dashboard()

        print(f"\nAll dashboards generated in '{self.output_dir}' directory")
        
        # Create index.html
        index_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>NYC TLC Data Analytics Dashboard</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .dashboard-section { margin-bottom: 30px; }
                h1, h2 { color: #333; }
                .dashboard-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px;
                    margin-top: 20px;
                }
                .dashboard-item {
                    border: 1px solid #ddd;
                    padding: 10px;
                    border-radius: 5px;
                }
                a { color: #0066cc; text-decoration: none; }
                a:hover { text-decoration: underline; }
            </style>
        </head>
        <body>
            <h1>NYC TLC Data Analytics Dashboard</h1>
            
            <div class="dashboard-section">
                <h2>Trip Volume Analysis</h2>
                <div class="dashboard-grid">
                    <div class="dashboard-item">
                        <h3>Hourly Patterns</h3>
                        <a href="hourly_patterns.html" target="_blank">View Dashboard</a>
                    </div>
                    <div class="dashboard-item">
                        <h3>Daily Patterns</h3>
                        <a href="daily_patterns.html" target="_blank">View Dashboard</a>
                    </div>
                </div>
            </div>
            
            <div class="dashboard-section">
                <h2>Financial Analysis</h2>
                <div class="dashboard-grid">
                    <div class="dashboard-item">
                        <h3>Payment Distribution</h3>
                        <a href="payment_distribution.html" target="_blank">View Dashboard</a>
                    </div>
                    <div class="dashboard-item">
                        <h3>Fare by Time</h3>
                        <a href="fare_by_time.html" target="_blank">View Dashboard</a>
                    </div>
                    <div class="dashboard-item">
                        <h3>Tip Patterns</h3>
                        <a href="tip_patterns.html" target="_blank">View Dashboard</a>
                    </div>
                </div>
            </div>
            
            <div class="dashboard-section">
                <h2>Geographic Analysis</h2>
                <div class="dashboard-grid">
                    <div class="dashboard-item">
                        <h3>Top Pickup Locations</h3>
                        <a href="top_pickups.html" target="_blank">View Dashboard</a>
                    </div>
                    <div class="dashboard-item">
                        <h3>Speed Patterns</h3>
                        <a href="speed_patterns.html" target="_blank">View Dashboard</a>
                    </div>
                </div>
            </div>
            
            <div class="dashboard-section">
                <h2>Performance Metrics</h2>
                <div class="dashboard-grid">
                    <div class="dashboard-item">
                        <h3>Distance Categories</h3>
                        <a href="distance_categories.html" target="_blank">View Dashboard</a>
                    </div>
                    <div class="dashboard-item">
                        <h3>Speed Distribution</h3>
                        <a href="speed_distribution.html" target="_blank">View Dashboard</a>
                    </div>
                    <div class="dashboard-item">
                        <h3>Duration Patterns</h3>
                        <a href="duration_patterns.html" target="_blank">View Dashboard</a>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        with open(f"{self.output_dir}/index.html", 'w') as f:
            f.write(index_html)

if __name__ == "__main__":
    generator = DashboardGenerator()
    generator.generate_all_dashboards()
