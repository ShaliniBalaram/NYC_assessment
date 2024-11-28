-- Trip Volume Analysis View
CREATE OR REPLACE VIEW `scenic-flux-441021-t4.nyc_tlc_data.dashboard_trip_volume` AS
WITH hourly_stats AS (
  SELECT
    pickup_hour,
    pickup_dayofweek,
    time_of_day,
    COUNT(*) as trip_count,
    AVG(total_amount) as avg_fare,
    AVG(trip_distance) as avg_distance
  FROM `scenic-flux-441021-t4.nyc_tlc_data.tlc_analytics_*`
  GROUP BY 1, 2, 3
)
SELECT * FROM hourly_stats;

-- Financial Metrics View
CREATE OR REPLACE VIEW `scenic-flux-441021-t4.nyc_tlc_data.dashboard_financial_metrics` AS
SELECT
  payment_type,
  time_of_day,
  is_weekend,
  COUNT(*) as trip_count,
  AVG(total_amount) as avg_fare,
  AVG(tip_percentage) as avg_tip_percentage,
  AVG(cost_per_mile) as avg_cost_per_mile
FROM `scenic-flux-441021-t4.nyc_tlc_data.tlc_analytics_*`
GROUP BY 1, 2, 3;

-- Geographic Distribution View
CREATE OR REPLACE VIEW `scenic-flux-441021-t4.nyc_tlc_data.dashboard_geographic_metrics` AS
SELECT
  pickup_location_id,
  dropoff_location_id,
  COUNT(*) as trip_count,
  AVG(trip_distance) as avg_distance,
  AVG(total_amount) as avg_fare,
  AVG(speed_mph) as avg_speed
FROM `scenic-flux-441021-t4.nyc_tlc_data.tlc_analytics_*`
GROUP BY 1, 2;

-- Trip Performance Metrics View
CREATE OR REPLACE VIEW `scenic-flux-441021-t4.nyc_tlc_data.dashboard_performance_metrics` AS
SELECT
  trip_distance_category,
  time_of_day,
  is_weekend,
  COUNT(*) as trip_count,
  AVG(speed_mph) as avg_speed,
  AVG(trip_duration_minutes) as avg_duration,
  AVG(total_amount) as avg_fare
FROM `scenic-flux-441021-t4.nyc_tlc_data.tlc_analytics_*`
GROUP BY 1, 2, 3;
