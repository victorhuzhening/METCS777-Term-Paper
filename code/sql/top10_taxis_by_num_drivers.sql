-- EXPLAIN ANALYZE
SELECT 
  medallion AS taxi_id,
  COUNT(DISTINCT(hack_license)) AS num_drivers
FROM 
  raw.nyc_taxi_trips
GROUP BY 
  medallion
ORDER BY 
  num_drivers DESC
LIMIT 10;