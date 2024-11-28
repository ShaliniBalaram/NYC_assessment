from google.cloud import bigquery
from typing import List

class BigQueryViewManager:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def create_view(self, view_name: str, query: str, dataset: str) -> None:
        """Create a BigQuery view"""
        view_id = f"{self.project_id}.{dataset}.{view_name}"
        view = bigquery.Table(view_id)
        view.view_query = query
        
        try:
            self.client.delete_table(view_id, not_found_ok=True)
            self.client.create_table(view)
            print(f"Created view {view_id}")
        except Exception as e:
            print(f"Error creating view {view_id}: {str(e)}")

    def create_visualization_views(self, year: int, month: int) -> None:
        """Create all visualization-ready views"""
        
        # 1. Hourly Trip Patterns View
        hourly_patterns_query = f"""
        SELECT
            pickup_hour,
            COUNT(*) as total_trips,
            AVG(total_amount) as avg_fare,
            AVG(trip_distance) as avg_distance,
            AVG(trip_duration_minutes) as avg_duration
        FROM `{self.project_id}.nyc_tlc_data.trips_{year}_{month:02d}`
        GROUP BY pickup_hour
        ORDER BY pickup_hour
        """
        self.create_view(f"viz_hourly_patterns_{year}_{month:02d}", 
                        hourly_patterns_query, "viz_views")

        # 2. Daily Revenue Trends View
        daily_revenue_query = f"""
        SELECT
            pickup_date,
            COUNT(*) as total_trips,
            SUM(total_amount) as total_revenue,
            AVG(tip_percentage) as avg_tip_percentage,
            SUM(CASE WHEN payment_type_desc = 'credit_card' THEN total_amount ELSE 0 END) as card_revenue,
            SUM(CASE WHEN payment_type_desc = 'cash' THEN total_amount ELSE 0 END) as cash_revenue
        FROM `{self.project_id}.nyc_tlc_data.trips_{year}_{month:02d}`
        GROUP BY pickup_date
        ORDER BY pickup_date
        """
        self.create_view(f"viz_daily_revenue_{year}_{month:02d}", 
                        daily_revenue_query, "viz_views")

        # 3. Trip Distance Distribution View
        distance_dist_query = f"""
        SELECT
            distance_category,
            COUNT(*) as trip_count,
            AVG(total_amount) as avg_fare,
            AVG(trip_duration_minutes) as avg_duration,
            AVG(tip_percentage) as avg_tip_percentage
        FROM `{self.project_id}.nyc_tlc_data.trips_{year}_{month:02d}`
        GROUP BY distance_category
        ORDER BY 
            CASE distance_category
                WHEN 'short' THEN 1
                WHEN 'medium' THEN 2
                WHEN 'long' THEN 3
                WHEN 'very_long' THEN 4
            END
        """
        self.create_view(f"viz_distance_distribution_{year}_{month:02d}", 
                        distance_dist_query, "viz_views")

        # 4. Payment Method Analysis View
        payment_analysis_query = f"""
        SELECT
            payment_type_desc,
            COUNT(*) as trip_count,
            AVG(total_amount) as avg_fare,
            AVG(tip_percentage) as avg_tip_percentage,
            AVG(trip_distance) as avg_distance
        FROM `{self.project_id}.nyc_tlc_data.trips_{year}_{month:02d}`
        GROUP BY payment_type_desc
        ORDER BY trip_count DESC
        """
        self.create_view(f"viz_payment_analysis_{year}_{month:02d}", 
                        payment_analysis_query, "viz_views")

        # 5. Time of Day Performance View
        time_performance_query = f"""
        SELECT
            time_of_day,
            COUNT(*) as total_trips,
            AVG(total_amount) as avg_fare,
            AVG(trip_duration_minutes) as avg_duration,
            AVG(speed_mph) as avg_speed,
            AVG(tip_percentage) as avg_tip_percentage
        FROM `{self.project_id}.nyc_tlc_data.trips_{year}_{month:02d}`
        GROUP BY time_of_day
        ORDER BY 
            CASE time_of_day
                WHEN 'morning_rush' THEN 1
                WHEN 'midday' THEN 2
                WHEN 'evening_rush' THEN 3
                WHEN 'night' THEN 4
                WHEN 'late_night' THEN 5
            END
        """
        self.create_view(f"viz_time_performance_{year}_{month:02d}", 
                        time_performance_query, "viz_views")

        # 6. Weekday vs Weekend Analysis View
        weekday_weekend_query = f"""
        SELECT
            CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as day_type,
            COUNT(*) as total_trips,
            AVG(total_amount) as avg_fare,
            AVG(trip_distance) as avg_distance,
            AVG(trip_duration_minutes) as avg_duration,
            AVG(tip_percentage) as avg_tip_percentage
        FROM `{self.project_id}.nyc_tlc_data.trips_{year}_{month:02d}`
        GROUP BY is_weekend
        """
        self.create_view(f"viz_weekday_weekend_{year}_{month:02d}", 
                        weekday_weekend_query, "viz_views")

        # 7. Location Performance View
        location_performance_query = f"""
        SELECT
            pu_location_id,
            COUNT(*) as total_pickups,
            AVG(total_amount) as avg_fare,
            AVG(trip_distance) as avg_distance,
            AVG(tip_percentage) as avg_tip_percentage,
            COUNT(CASE WHEN time_of_day = 'morning_rush' THEN 1 END) as morning_rush_trips,
            COUNT(CASE WHEN time_of_day = 'evening_rush' THEN 1 END) as evening_rush_trips
        FROM `{self.project_id}.nyc_tlc_data.trips_{year}_{month:02d}`
        GROUP BY pu_location_id
        ORDER BY total_pickups DESC
        """
        self.create_view(f"viz_location_performance_{year}_{month:02d}", 
                        location_performance_query, "viz_views")

    def create_datasets(self) -> None:
        """Create necessary datasets if they don't exist"""
        datasets = ['viz_views']
        
        for dataset in datasets:
            dataset_id = f"{self.project_id}.{dataset}"
            try:
                dataset_ref = bigquery.Dataset(dataset_id)
                dataset_ref.location = "US"
                self.client.create_dataset(dataset_ref, exists_ok=True)
                print(f"Dataset {dataset_id} is ready")
            except Exception as e:
                print(f"Error creating dataset {dataset_id}: {str(e)}")
