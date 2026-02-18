with duplicate_keys as (
    select
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        total_amount
    from {{ ref('int_trips_unioned') }}
    group by 1,2,3,4,5,6
    having count(*) > 1
    limit 20
)

select t.*
from {{ ref('int_trips_unioned') }} t
join duplicate_keys d
  on  t.vendor_id = d.vendor_id
  and t.pickup_datetime = d.pickup_datetime
  and t.dropoff_datetime = d.dropoff_datetime
  and t.pickup_location_id = d.pickup_location_id
  and t.dropoff_location_id = d.dropoff_location_id
  and t.total_amount = d.total_amount
order by
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.total_amount;