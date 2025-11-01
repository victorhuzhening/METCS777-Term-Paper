-- EXPLAIN ANALYZE
WITH hourly AS (
  SELECT
    EXTRACT(HOUR FROM pickup_datetime)::int AS hour_of_day,  
    SUM(surcharge)::numeric AS total_surcharge_usd,
    SUM(trip_distance)::numeric AS total_miles,
    COUNT(*) AS trips
  FROM raw.nyc_taxi_trips
  WHERE pickup_datetime IS NOT NULL
    AND surcharge IS NOT NULL
    AND trip_distance IS NOT NULL
    AND trip_distance >= 0
  GROUP BY 1
),
hours AS (
  SELECT generate_series(0, 23)::int AS hour_of_day
),
per_hour AS (
  SELECT
    h.hour_of_day,
    COALESCE(hl.trips, 0) AS trips,
    COALESCE(hl.total_surcharge_usd, 0) AS total_surcharge_usd,
    COALESCE(hl.total_miles, 0) AS total_miles,
    (COALESCE(hl.total_surcharge_usd, 0) / NULLIF(COALESCE(hl.total_miles, 0), 0)) AS profit_ratio
  FROM hours h
  LEFT JOIN hourly hl USING (hour_of_day)
)

SELECT
  hour_of_day,
  ROUND(profit_ratio, 4) AS profit_ratio
FROM per_hour
ORDER BY profit_ratio DESC
LIMIT 10;