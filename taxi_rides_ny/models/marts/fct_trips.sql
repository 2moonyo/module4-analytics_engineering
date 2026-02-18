/*
One row per trip — yellow and green combined (the union is already done in the intermediate model)
Add a primary key (trip_id) — it has to be unique
Find and fix duplicates — there are quite a few in this dataset. Some come from the source, some get introduced during the union. Find them, understand why they happen, and fix them
Enrich payment_type (there is a seed for this in the repo).

NOTE: Using table materialization for initial build to avoid OOM errors.
Change back to incremental after first successful build.
*/

{{
  config(
    materialized='table'
  )
}}

 
select
    trips.trip_id,
    trips.vendor_id,
    trips.taxi_type,
    trips.rate_code_id,

    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,

    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,

    -- DuckDB-native duration in minutes
    date_diff('minute', trips.pickup_datetime, trips.dropoff_datetime) as trip_duration_minutes,

    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from {{ ref('int_trips') }} as trips
left join {{ ref('dim_zones') }} as pz
  on trips.pickup_location_id = pz.location_id
left join {{ ref('dim_zones') }} as dz
  on trips.dropoff_location_id = dz.location_id
