import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery
from typing import Dict, List, Tuple, Union
import plotly.express as px
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

class TLCExploratoryAnalysis:
    def __init__(self, project_id: str, dataset_id: str = "nyc_tlc_data"):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.set_plotting_style()
    
    def set_plotting_style(self):
        """Set consistent plotting style for all visualizations"""
        plt.style.use('seaborn')
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = (12, 6)
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['axes.labelsize'] = 12
    
    def fetch_data(self, year: int, month: int) -> pd.DataFrame:
        """Fetch data from BigQuery for analysis"""
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.tlc_analytics_{year}_{month:02d}`
        """
        return self.client.query(query).to_dataframe()

    def analyze_numerical_distributions(self, df: pd.DataFrame, save_path: str = None):
        """Analyze distributions of numerical variables"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        n_cols = len(numerical_cols)
        n_rows = (n_cols + 1) // 2
        
        fig, axes = plt.subplots(n_rows, 2, figsize=(15, 5*n_rows))
        axes = axes.flatten()
        
        for i, col in enumerate(numerical_cols):
            # Distribution plot
            sns.histplot(data=df, x=col, kde=True, ax=axes[i])
            axes[i].set_title(f'Distribution of {col}')
            
            # Add summary statistics
            stats_text = f'Mean: {df[col].mean():.2f}\n'
            stats_text += f'Median: {df[col].median():.2f}\n'
            stats_text += f'Std: {df[col].std():.2f}\n'
            stats_text += f'Skew: {df[col].skew():.2f}'
            
            axes[i].text(0.95, 0.95, stats_text,
                        transform=axes[i].transAxes,
                        verticalalignment='top',
                        horizontalalignment='right',
                        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # Remove empty subplots
        for i in range(i+1, len(axes)):
            fig.delaxes(axes[i])
        
        plt.tight_layout()
        if save_path:
            plt.savefig(f"{save_path}/numerical_distributions.png")
        plt.close()

    def analyze_temporal_patterns(self, df: pd.DataFrame, save_path: str = None):
        """Analyze temporal patterns in the data"""
        # Create figure with subplots
        fig = plt.figure(figsize=(20, 15))
        
        # 1. Hourly patterns
        plt.subplot(2, 2, 1)
        hourly_trips = df.groupby('pickup_hour')['trip_id'].count()
        sns.lineplot(x=hourly_trips.index, y=hourly_trips.values)
        plt.title('Trips by Hour of Day')
        plt.xlabel('Hour')
        plt.ylabel('Number of Trips')
        
        # 2. Day of week patterns
        plt.subplot(2, 2, 2)
        daily_trips = df.groupby('pickup_dayofweek')['trip_id'].count()
        sns.barplot(x=daily_trips.index, y=daily_trips.values)
        plt.title('Trips by Day of Week')
        plt.xlabel('Day (0=Sunday)')
        plt.ylabel('Number of Trips')
        
        # 3. Time of day comparison
        plt.subplot(2, 2, 3)
        sns.boxplot(data=df, x='time_of_day', y='total_amount')
        plt.title('Fare Distribution by Time of Day')
        plt.xticks(rotation=45)
        
        # 4. Weekend vs Weekday
        plt.subplot(2, 2, 4)
        sns.violinplot(data=df, x='is_weekend', y='total_amount')
        plt.title('Fare Distribution: Weekday vs Weekend')
        
        plt.tight_layout()
        if save_path:
            plt.savefig(f"{save_path}/temporal_patterns.png")
        plt.close()

    def analyze_spatial_patterns(self, df: pd.DataFrame, save_path: str = None):
        """Analyze spatial patterns in the data"""
        # Create figure with subplots
        fig = plt.figure(figsize=(20, 15))
        
        # 1. Top pickup locations
        plt.subplot(2, 2, 1)
        top_pickups = df['pickup_location_id'].value_counts().head(10)
        sns.barplot(x=top_pickups.values, y=top_pickups.index)
        plt.title('Top 10 Pickup Locations')
        plt.xlabel('Number of Trips')
        
        # 2. Distance distribution by time of day
        plt.subplot(2, 2, 2)
        sns.boxplot(data=df, x='time_of_day', y='trip_distance')
        plt.title('Trip Distance by Time of Day')
        plt.xticks(rotation=45)
        
        # 3. Speed patterns
        plt.subplot(2, 2, 3)
        sns.scatterplot(data=df.sample(1000), x='trip_distance', y='speed_mph', 
                       hue='time_of_day', alpha=0.6)
        plt.title('Speed vs Distance by Time of Day')
        
        # 4. Distance category distribution
        plt.subplot(2, 2, 4)
        sns.countplot(data=df, x='trip_distance_category')
        plt.title('Trip Distance Categories')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        if save_path:
            plt.savefig(f"{save_path}/spatial_patterns.png")
        plt.close()

    def analyze_financial_patterns(self, df: pd.DataFrame, save_path: str = None):
        """Analyze financial patterns in the data"""
        # Create figure with subplots
        fig = plt.figure(figsize=(20, 15))
        
        # 1. Fare distribution by payment type
        plt.subplot(2, 2, 1)
        sns.boxplot(data=df, x='payment_type', y='total_amount')
        plt.title('Fare Distribution by Payment Type')
        plt.xticks(rotation=45)
        
        # 2. Tip percentage by time of day
        plt.subplot(2, 2, 2)
        sns.boxplot(data=df, x='time_of_day', y='tip_percentage')
        plt.title('Tip Percentage by Time of Day')
        plt.xticks(rotation=45)
        
        # 3. Fare vs Distance
        plt.subplot(2, 2, 3)
        sns.scatterplot(data=df.sample(1000), x='trip_distance', y='total_amount',
                       hue='payment_type', alpha=0.6)
        plt.title('Fare vs Distance by Payment Type')
        
        # 4. Cost per mile distribution
        plt.subplot(2, 2, 4)
        sns.histplot(data=df, x='cost_per_mile', bins=50)
        plt.title('Cost per Mile Distribution')
        
        plt.tight_layout()
        if save_path:
            plt.savefig(f"{save_path}/financial_patterns.png")
        plt.close()

    def analyze_correlations(self, df: pd.DataFrame, save_path: str = None):
        """Analyze correlations between numerical variables"""
        # Select numerical columns
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        
        # Calculate correlation matrix
        corr_matrix = df[numerical_cols].corr()
        
        # Create heatmap
        plt.figure(figsize=(12, 10))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0)
        plt.title('Correlation Matrix of Numerical Variables')
        
        if save_path:
            plt.savefig(f"{save_path}/correlations.png")
        plt.close()

    def identify_anomalies(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Identify anomalies in the data"""
        anomalies = {}
        
        # Function to detect outliers using IQR method
        def get_outliers(series):
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            return series[(series < lower_bound) | (series > upper_bound)]
        
        # Check for anomalies in key metrics
        metrics = ['total_amount', 'trip_distance', 'trip_duration_minutes', 'speed_mph']
        
        for metric in metrics:
            outliers = get_outliers(df[metric])
            if len(outliers) > 0:
                anomalies[metric] = df[df[metric].isin(outliers)]
        
        return anomalies

    def generate_summary_report(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Generate a comprehensive summary report"""
        summary = {}
        
        # Basic statistics
        summary['basic_stats'] = df.describe()
        
        # Temporal patterns
        summary['temporal'] = {
            'hourly_patterns': df.groupby('pickup_hour')['trip_id'].count().to_dict(),
            'daily_patterns': df.groupby('pickup_dayofweek')['trip_id'].count().to_dict(),
            'weekend_vs_weekday': df.groupby('is_weekend')['trip_id'].count().to_dict()
        }
        
        # Financial patterns
        summary['financial'] = {
            'payment_distribution': df['payment_type'].value_counts().to_dict(),
            'avg_fare_by_time': df.groupby('time_of_day')['total_amount'].mean().to_dict(),
            'avg_tip_by_time': df.groupby('time_of_day')['tip_percentage'].mean().to_dict()
        }
        
        # Distance patterns
        summary['distance'] = {
            'distance_categories': df['trip_distance_category'].value_counts().to_dict(),
            'avg_speed_by_time': df.groupby('time_of_day')['speed_mph'].mean().to_dict()
        }
        
        return summary

    def perform_eda(self, year: int, month: int, save_path: str = None) -> Dict:
        """Perform complete exploratory data analysis"""
        print(f"Starting EDA for {year}-{month:02d}")
        
        # Fetch data
        df = self.fetch_data(year, month)
        print(f"Loaded {len(df)} records")
        
        # Create save directory if needed
        if save_path:
            import os
            os.makedirs(save_path, exist_ok=True)
        
        # Perform analyses
        print("Analyzing numerical distributions...")
        self.analyze_numerical_distributions(df, save_path)
        
        print("Analyzing temporal patterns...")
        self.analyze_temporal_patterns(df, save_path)
        
        print("Analyzing spatial patterns...")
        self.analyze_spatial_patterns(df, save_path)
        
        print("Analyzing financial patterns...")
        self.analyze_financial_patterns(df, save_path)
        
        print("Analyzing correlations...")
        self.analyze_correlations(df, save_path)
        
        print("Identifying anomalies...")
        anomalies = self.identify_anomalies(df)
        
        print("Generating summary report...")
        summary = self.generate_summary_report(df)
        
        return {
            'summary': summary,
            'anomalies': anomalies,
            'record_count': len(df)
        }

def example_usage():
    """Example usage of the TLCExploratoryAnalysis class"""
    # Initialize analyzer
    analyzer = TLCExploratoryAnalysis("scenic-flux-441021-t4")
    
    # Set analysis period
    year = 2024
    month = 1
    
    # Perform EDA and save visualizations
    results = analyzer.perform_eda(year, month, save_path="eda_results")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(results['summary']['basic_stats'])
    
    # Print anomaly counts
    print("\nAnomalies Detected:")
    for metric, anomalies in results['anomalies'].items():
        print(f"{metric}: {len(anomalies)} anomalies")

if __name__ == "__main__":
    example_usage()
