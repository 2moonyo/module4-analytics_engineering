with hashed as (
  select
    md5(concat(
      cast(vendor_id as varchar),
      cast(pickup_datetime as varchar),
      cast(dropoff_datetime as varchar),
      cast(pickup_location_id as varchar),
      cast(dropoff_location_id as varchar),
      cast(passenger_count as varchar),
      cast(trip_distance as varchar),
      cast(fare_amount as varchar),
      cast(tip_amount as varchar),
      cast(total_amount as varchar),
      cast(payment_type as varchar)
    )) as row_hash,
    *
  from {{ ref('int_trips_unioned') }}
)

select *
from hashed
where row_hash = '316633a5f213ffad798ed718c909a5ab'
order by pickup_datetime;
