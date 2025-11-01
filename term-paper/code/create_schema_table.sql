CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.nyc_taxi_trips (
  medallion TEXT,
  hack_license TEXT,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  trip_time_in_secs INTEGER,
  trip_distance DOUBLE PRECISION,
  pickup_longitude DOUBLE PRECISION,
  pickup_latitude DOUBLE PRECISION,
  dropoff_longitude DOUBLE PRECISION,
  dropoff_latitude DOUBLE PRECISION,
  payment_type TEXT,
  fare_amount DOUBLE PRECISION,
  surcharge DOUBLE PRECISION,
  mta_tax DOUBLE PRECISION,
  tip_amount DOUBLE PRECISION,
  tolls_amount DOUBLE PRECISION,
  total_amount DOUBLE PRECISION
);
