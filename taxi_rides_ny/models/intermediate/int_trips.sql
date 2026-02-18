{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

with unioned as (
    select * from {{ ref('int_trips_unioned') }}
    
),

payment_types as (
    select * from {{ ref('dim_payment_type') }}
),

-- Identify void trip groups (trips with both positive and negative fares)
-- These are canceled/voided transactions that appear as matching positive/negative pairs
void_groups as (
    select
        md5(concat(
            coalesce(cast(vendor_id as varchar), ''), '|',
            coalesce(cast(pickup_datetime as varchar), ''), '|',
            coalesce(cast(pickup_location_id as varchar), ''), '|',
            coalesce(cast(taxi_type as varchar), '')
        )) as trip_identity_key,
        max(case when fare_amount > 0 then 1 else 0 end) as has_positive,
        max(case when fare_amount < 0 then 1 else 0 end) as has_negative
    from unioned
    group by 1
),

enriched as (
    select
        -- Trip identity used for dedupe grouping - create a hash of the fields that define a trip (except dropoff_datetime)
        md5(concat(
            coalesce(cast(u.vendor_id as varchar), ''), '|',
            coalesce(cast(u.pickup_datetime as varchar), ''), '|',
            coalesce(cast(u.pickup_location_id as varchar), ''), '|',
            coalesce(cast(u.taxi_type as varchar), '')
        )) as trip_identity_key,

        -- Keep all fields 
        u.*,

        -- Payment enrichment
        coalesce(u.payment_type, 0) as payment_type_clean,
        coalesce(pt.payment_type_description, 'Unknown') as payment_type_description

    from unioned u
    left join payment_types pt
      on coalesce(u.payment_type, 0) = pt.payment_type_id
    left join void_groups vg
      on md5(concat(
            coalesce(cast(u.vendor_id as varchar), ''), '|',
            coalesce(cast(u.pickup_datetime as varchar), ''), '|',
            coalesce(cast(u.pickup_location_id as varchar), ''), '|',
            coalesce(cast(u.taxi_type as varchar), '')
        )) = vg.trip_identity_key
    -- Exclude void trips (those with both positive and negative fares)
    where not (vg.has_positive = 1 and vg.has_negative = 1)
),

-- pick one “winner” per identity group (deterministic)
winners as (
    select
        trip_identity_key,
        min(dropoff_datetime) as keep_dropoff_datetime
    from enriched
    group by 1
),

deduped as (
    select e.*
    from enriched e
    join winners w
      on e.trip_identity_key = w.trip_identity_key
     and e.dropoff_datetime = w.keep_dropoff_datetime
)

select
    -- Surrogate key (trip_id) from the deduped set
    md5(concat(
        coalesce(cast(vendor_id as varchar), ''), '|',
        coalesce(cast(pickup_datetime as varchar), ''), '|',
        coalesce(cast(pickup_location_id as varchar), ''), '|',
        coalesce(cast(taxi_type as varchar), '')
    )) as trip_id,

    -- identifiers
    vendor_id,
    taxi_type,
    rate_code_id,

    -- locations
    pickup_location_id,
    dropoff_location_id,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,

    -- trip details
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,

    -- payment breakdown 
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,

    -- enriched payment type
    payment_type_clean as payment_type,
    payment_type_description

from deduped