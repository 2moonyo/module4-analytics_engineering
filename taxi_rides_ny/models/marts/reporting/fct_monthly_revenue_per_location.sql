{{
  config(
    materialized='table'
  )
}}

with fct_trips as (
    select * from {{ ref('fct_trips') }}
)

select
    -- Time dimension
    date_trunc('month', pickup_datetime) as revenue_month,
    
    -- Location dimension
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    
    -- Taxi type
    taxi_type,
    
    -- Aggregated metrics
    count(trip_id) as total_trips,
    
    -- Revenue metrics
    sum(total_amount) as total_revenue,
    sum(fare_amount) as total_fare_amount,
    sum(extra) as total_extra,
    sum(mta_tax) as total_mta_tax,
    sum(tip_amount) as total_tip_amount,
    sum(tolls_amount) as total_tolls_amount,
    sum(improvement_surcharge) as total_improvement_surcharge,
    
    -- Average metrics
    avg(total_amount) as avg_revenue_per_trip,
    avg(fare_amount) as avg_fare_amount,
    avg(tip_amount) as avg_tip_amount,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    avg(passenger_count) as avg_passenger_count,
    
    -- Trip characteristics
    sum(trip_distance) as total_distance,
    sum(passenger_count) as total_passengers

from fct_trips
group by
    date_trunc('month', pickup_datetime),
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    taxi_type    