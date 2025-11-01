-- EXPLAIN ANALYZE
SELECT 
  hack_license AS driver, 
  SUM(total_amount) / NULLIF(SUM(trip_time_in_secs) / 60.0, 0) AS money_per_minute
FROM 
  raw.nyc_taxi_trips
WHERE 
  trip_time_in_secs > 0
GROUP BY 
  hack_license
ORDER BY 
  money_per_minute DESC
LIMIT 10;