from google.cloud import bigquery
from typing import Dict, List, Union
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from scipy import stats
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

class TLCDataAnalyzer:
    def __init__(self, project_id: str, dataset_id: str = "nyc_tlc_data"):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id

    def run_query(self, query: str) -> pd.DataFrame:
        """Execute a BigQuery query and return results as a pandas DataFrame"""
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            raise

    def hourly_demand_analysis(self, year: int, month: int) -> pd.DataFrame:
        """Analyze hourly demand patterns"""
        query = f"""
        SELECT
            pickup_hour,
            COUNT(*) as total_trips,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(trip_duration_minutes), 2) as avg_duration,
            ROUND(AVG(speed_mph), 2) as avg_speed
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY pickup_hour
        ORDER BY pickup_hour
        """
        return self.run_query(query)

    def peak_hours_analysis(self, year: int, month: int) -> pd.DataFrame:
        """Identify and analyze peak hours"""
        query = f"""
        WITH hourly_stats AS (
            SELECT
                pickup_hour,
                COUNT(*) as trip_count,
                AVG(total_amount) as avg_fare,
                AVG(tip_percentage) as avg_tip_percent
            FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
            GROUP BY pickup_hour
        ),
        stats AS (
            SELECT
                AVG(trip_count) as avg_trips,
                STDDEV(trip_count) as stddev_trips
            FROM hourly_stats
        )
        SELECT
            h.*,
            CASE
                WHEN h.trip_count > (s.avg_trips + s.stddev_trips) THEN 'High'
                WHEN h.trip_count < (s.avg_trips - s.stddev_trips) THEN 'Low'
                ELSE 'Normal'
            END as demand_level
        FROM hourly_stats h
        CROSS JOIN stats s
        ORDER BY pickup_hour
        """
        return self.run_query(query)

    def revenue_by_location(self, year: int, month: int, top_n: int = 10) -> pd.DataFrame:
        """Analyze revenue patterns by pickup location"""
        query = f"""
        SELECT
            pickup_location_id,
            COUNT(*) as total_trips,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(tip_percentage), 2) as avg_tip_percent
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY pickup_location_id
        ORDER BY total_revenue DESC
        LIMIT {top_n}
        """
        return self.run_query(query)

    def payment_patterns(self, year: int, month: int) -> pd.DataFrame:
        """Analyze payment patterns and preferences"""
        query = f"""
        SELECT
            payment_type,
            COUNT(*) as total_trips,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(tip_percentage), 2) as avg_tip_percent,
            ROUND(AVG(total_amount), 2) as avg_fare,
            COUNT(CASE WHEN is_weekend = TRUE THEN 1 END) as weekend_trips,
            COUNT(CASE WHEN is_rush_hour = TRUE THEN 1 END) as rush_hour_trips
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY payment_type
        ORDER BY total_trips DESC
        """
        return self.run_query(query)

    def trip_distance_analysis(self, year: int, month: int) -> pd.DataFrame:
        """Analyze trip distance patterns"""
        query = f"""
        SELECT
            trip_distance_category,
            COUNT(*) as total_trips,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(trip_duration_minutes), 2) as avg_duration,
            ROUND(AVG(speed_mph), 2) as avg_speed,
            ROUND(AVG(tip_percentage), 2) as avg_tip_percent
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY trip_distance_category
        ORDER BY 
            CASE trip_distance_category
                WHEN 'short' THEN 1
                WHEN 'medium' THEN 2
                WHEN 'long' THEN 3
                WHEN 'very_long' THEN 4
            END
        """
        return self.run_query(query)

    def weekday_weekend_comparison(self, year: int, month: int) -> pd.DataFrame:
        """Compare weekday vs weekend patterns"""
        query = f"""
        SELECT
            CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
            COUNT(*) as total_trips,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(trip_duration_minutes), 2) as avg_duration,
            ROUND(AVG(tip_percentage), 2) as avg_tip_percent,
            ROUND(AVG(speed_mph), 2) as avg_speed
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY is_weekend
        """
        return self.run_query(query)

    def rush_hour_impact(self, year: int, month: int) -> pd.DataFrame:
        """Analyze the impact of rush hours"""
        query = f"""
        SELECT
            time_of_day,
            COUNT(*) as total_trips,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(speed_mph), 2) as avg_speed,
            ROUND(AVG(trip_duration_minutes), 2) as avg_duration,
            ROUND(AVG(tip_percentage), 2) as avg_tip_percent
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY time_of_day
        ORDER BY 
            CASE time_of_day
                WHEN 'early_morning' THEN 1
                WHEN 'morning_rush' THEN 2
                WHEN 'midday' THEN 3
                WHEN 'evening_rush' THEN 4
                WHEN 'night' THEN 5
            END
        """
        return self.run_query(query)

    def create_visualizations(self, year: int, month: int) -> Dict[str, go.Figure]:
        """Create a set of visualizations for the analyzed data"""
        figures = {}
        
        # Hourly Demand Pattern
        hourly_data = self.hourly_demand_analysis(year, month)
        fig = px.line(hourly_data, x='pickup_hour', y='total_trips',
                     title='Hourly Trip Demand Pattern')
        figures['hourly_demand'] = fig
        
        # Payment Type Distribution
        payment_data = self.payment_patterns(year, month)
        fig = px.pie(payment_data, values='total_trips', names='payment_type',
                    title='Distribution of Payment Types')
        figures['payment_distribution'] = fig
        
        # Trip Distance Categories
        distance_data = self.trip_distance_analysis(year, month)
        fig = px.bar(distance_data, x='trip_distance_category', y='total_trips',
                    title='Trip Distribution by Distance Category')
        figures['distance_distribution'] = fig
        
        # Rush Hour Analysis
        rush_hour_data = self.rush_hour_impact(year, month)
        fig = px.bar(rush_hour_data, x='time_of_day', y=['avg_speed', 'avg_fare'],
                    title='Speed and Fare by Time of Day',
                    barmode='group')
        figures['rush_hour_impact'] = fig
        
        return figures

    def generate_summary_report(self, year: int, month: int) -> Dict[str, Union[float, int]]:
        """Generate a summary report with key metrics"""
        query = f"""
        SELECT
            COUNT(*) as total_trips,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(trip_distance), 2) as avg_distance,
            ROUND(AVG(trip_duration_minutes), 2) as avg_duration,
            ROUND(AVG(tip_percentage), 2) as avg_tip_percent,
            ROUND(AVG(speed_mph), 2) as avg_speed,
            COUNT(DISTINCT pickup_location_id) as unique_pickup_locations,
            COUNT(DISTINCT dropoff_location_id) as unique_dropoff_locations
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        """
        return self.run_query(query).to_dict('records')[0]

    def analyze_trends(self, start_year: int, start_month: int, end_year: int, end_month: int) -> pd.DataFrame:
        """Analyze trends across multiple months"""
        months = []
        current_year, current_month = start_year, start_month
        
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            months.append((current_year, current_month))
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
        
        query = f"""
        WITH monthly_metrics AS (
            {" UNION ALL ".join(f'''
            SELECT
                DATE('{year}-{month:02d}-01') as month,
                COUNT(*) as total_trips,
                ROUND(SUM(total_amount), 2) as total_revenue,
                ROUND(AVG(total_amount), 2) as avg_fare,
                ROUND(AVG(trip_distance), 2) as avg_distance,
                ROUND(AVG(trip_duration_minutes), 2) as avg_duration,
                ROUND(AVG(tip_percentage), 2) as avg_tip_percent
            FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
            ''' for year, month in months)}
        )
        SELECT *,
            LAG(total_trips) OVER (ORDER BY month) as prev_month_trips,
            LAG(total_revenue) OVER (ORDER BY month) as prev_month_revenue
        FROM monthly_metrics
        ORDER BY month
        """
        return self.run_query(query)

    def seasonal_analysis(self, df: pd.DataFrame, column: str) -> Dict[str, Union[pd.DataFrame, float]]:
        """Perform seasonal decomposition and statistical tests"""
        # Convert to time series
        ts = df.set_index('month')[column]
        
        # Perform seasonal decomposition
        decomposition = seasonal_decompose(ts, period=12, extrapolate_trend='freq')
        
        # Perform Augmented Dickey-Fuller test for stationarity
        adf_test = adfuller(ts)
        
        return {
            'trend': decomposition.trend,
            'seasonal': decomposition.seasonal,
            'residual': decomposition.resid,
            'adf_statistic': adf_test[0],
            'adf_pvalue': adf_test[1]
        }

    def location_clustering(self, year: int, month: int, n_clusters: int = 5) -> pd.DataFrame:
        """Perform clustering analysis on pickup locations"""
        query = f"""
        SELECT
            pickup_location_id,
            COUNT(*) as total_trips,
            AVG(total_amount) as avg_fare,
            AVG(trip_distance) as avg_distance,
            AVG(tip_percentage) as avg_tip_percent,
            AVG(speed_mph) as avg_speed
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY pickup_location_id
        """
        df = self.run_query(query)
        
        # Prepare data for clustering
        features = ['total_trips', 'avg_fare', 'avg_distance', 'avg_tip_percent', 'avg_speed']
        X = df[features].values
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Perform clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        df['cluster'] = kmeans.fit_predict(X_scaled)
        
        return df

    def create_advanced_visualizations(self, year: int, month: int) -> Dict[str, go.Figure]:
        """Create advanced visualizations for deeper analysis"""
        figures = {}
        
        # 1. Heatmap of hourly demand by day of week
        query = f"""
        SELECT
            pickup_hour,
            EXTRACT(DAYOFWEEK FROM pickup_date) as day_of_week,
            COUNT(*) as trip_count
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY pickup_hour, day_of_week
        ORDER BY day_of_week, pickup_hour
        """
        heatmap_data = self.run_query(query)
        pivot_data = heatmap_data.pivot(index='day_of_week', columns='pickup_hour', values='trip_count')
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot_data.values,
            x=pivot_data.columns,
            y=['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
            colorscale='Viridis'
        ))
        fig.update_layout(title='Trip Demand Heatmap by Hour and Day',
                         xaxis_title='Hour of Day',
                         yaxis_title='Day of Week')
        figures['demand_heatmap'] = fig
        
        # 2. Box plots for fare distribution by time of day
        query = f"""
        SELECT time_of_day, total_amount
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        """
        box_data = self.run_query(query)
        
        fig = px.box(box_data, x='time_of_day', y='total_amount',
                    title='Fare Distribution by Time of Day')
        figures['fare_distribution'] = fig
        
        # 3. Scatter plot of distance vs. duration with trend line
        query = f"""
        SELECT
            trip_distance,
            trip_duration_minutes,
            total_amount,
            time_of_day
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        LIMIT 10000
        """
        scatter_data = self.run_query(query)
        
        fig = px.scatter(scatter_data,
                        x='trip_distance',
                        y='trip_duration_minutes',
                        color='time_of_day',
                        size='total_amount',
                        trendline="ols",
                        title='Trip Distance vs Duration')
        figures['distance_duration_relationship'] = fig
        
        # 4. Radar chart for time of day metrics
        query = f"""
        SELECT
            time_of_day,
            AVG(speed_mph) as avg_speed,
            AVG(total_amount) as avg_fare,
            AVG(tip_percentage) as avg_tip,
            AVG(trip_distance) as avg_distance
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        GROUP BY time_of_day
        """
        radar_data = self.run_query(query)
        
        # Normalize the data for radar chart
        scaler = StandardScaler()
        radar_data_scaled = pd.DataFrame(
            scaler.fit_transform(radar_data.select_dtypes(include=[np.number])),
            columns=radar_data.select_dtypes(include=[np.number]).columns
        )
        radar_data_scaled['time_of_day'] = radar_data['time_of_day']
        
        fig = go.Figure()
        for time in radar_data_scaled['time_of_day'].unique():
            data = radar_data_scaled[radar_data_scaled['time_of_day'] == time]
            fig.add_trace(go.Scatterpolar(
                r=data.iloc[0, :-1],
                theta=['Speed', 'Fare', 'Tip %', 'Distance'],
                name=time,
                fill='toself'
            ))
        
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[-2, 2])),
            showlegend=True,
            title='Time of Day Performance Metrics'
        )
        figures['time_performance_radar'] = fig
        
        return figures

    def perform_statistical_analysis(self, year: int, month: int) -> Dict[str, Dict[str, float]]:
        """Perform comprehensive statistical analysis"""
        results = {}
        
        # 1. Correlation Analysis
        query = f"""
        SELECT
            total_amount,
            trip_distance,
            trip_duration_minutes,
            tip_percentage,
            speed_mph
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        """
        df = self.run_query(query)
        correlation_matrix = df.corr()
        results['correlations'] = correlation_matrix.to_dict()
        
        # 2. Basic Statistics
        stats_dict = {}
        for column in df.columns:
            stats_dict[column] = {
                'mean': df[column].mean(),
                'median': df[column].median(),
                'std': df[column].std(),
                'skew': df[column].skew(),
                'kurtosis': df[column].kurtosis()
            }
        results['basic_stats'] = stats_dict
        
        # 3. Hypothesis Tests
        # Compare weekday vs weekend fares
        query = f"""
        SELECT
            is_weekend,
            total_amount
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        """
        df = self.run_query(query)
        weekday_fares = df[~df['is_weekend']]['total_amount']
        weekend_fares = df[df['is_weekend']]['total_amount']
        
        t_stat, p_value = stats.ttest_ind(weekday_fares, weekend_fares)
        results['weekday_weekend_ttest'] = {
            't_statistic': t_stat,
            'p_value': p_value
        }
        
        return results

    def analyze_monthly_trends(self, df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """Analyze trends in monthly data"""
        results = {}
        
        # Calculate month-over-month growth rates
        df['trip_growth'] = (df['total_trips'] - df['prev_month_trips']) / df['prev_month_trips'] * 100
        df['revenue_growth'] = (df['total_revenue'] - df['prev_month_revenue']) / df['prev_month_revenue'] * 100
        
        # Calculate moving averages
        df['trips_ma_3'] = df['total_trips'].rolling(window=3).mean()
        df['revenue_ma_3'] = df['total_revenue'].rolling(window=3).mean()
        
        # Perform trend analysis
        for metric in ['total_trips', 'total_revenue', 'avg_fare']:
            # Linear regression for trend
            X = np.arange(len(df)).reshape(-1, 1)
            y = df[metric].values
            slope, intercept, r_value, p_value, std_err = stats.linregress(X.flatten(), y)
            
            results[metric] = {
                'slope': slope,
                'r_squared': r_value**2,
                'p_value': p_value,
                'mean': df[metric].mean(),
                'std': df[metric].std(),
                'min': df[metric].min(),
                'max': df[metric].max()
            }
        
        return results

def example_usage():
    """Example usage of the TLCDataAnalyzer class"""
    # Initialize analyzer
    analyzer = TLCDataAnalyzer("scenic-flux-441021-t4")
    
    # Set analysis period
    year = 2024
    month = 1
    
    # Generate summary report
    summary = analyzer.generate_summary_report(year, month)
    print("\nSummary Report:")
    for metric, value in summary.items():
        print(f"{metric}: {value}")
    
    # Analyze hourly patterns
    hourly_patterns = analyzer.hourly_demand_analysis(year, month)
    print("\nHourly Demand Patterns:")
    print(hourly_patterns.head())
    
    # Analyze payment patterns
    payment_analysis = analyzer.payment_patterns(year, month)
    print("\nPayment Patterns:")
    print(payment_analysis)
    
    # Create visualizations
    figures = analyzer.create_visualizations(year, month)
    
    # Save visualizations (optional)
    for name, fig in figures.items():
        fig.write_html(f"visualization_{name}_{year}_{month}.html")

    # Analyze trends across multiple months
    trends_df = analyzer.analyze_trends(2024, 1, 2024, 3)
    trend_analysis = analyzer.analyze_monthly_trends(trends_df)
    
    # Create advanced visualizations
    advanced_figures = analyzer.create_advanced_visualizations(2024, 1)
    
    # Perform statistical analysis
    stats_results = analyzer.perform_statistical_analysis(2024, 1)
    
    # Save visualizations
    for name, fig in advanced_figures.items():
        fig.write_html(f"advanced_viz_{name}_2024_01.html")

if __name__ == "__main__":
    example_usage()
