with dup_trip_ids as (
    select
        trip_id,
        count(*) as cnt
    from {{ ref('fct_trips') }}
    group by 1
    having count(*) > 1
    order by cnt desc
    limit 20
)

select
    d.cnt,
    ft.trip_id,
    ft.vendor_id,
    ft.pickup_datetime,
    ft.dropoff_datetime,
    ft.pickup_location_id,
    ft.dropoff_location_id,
    ft.passenger_count,
    ft.trip_distance,
    ft.total_amount,
    ft.payment_type
    
from {{ ref('fct_trips') }} ft
join dup_trip_ids d
  on ft.trip_id = d.trip_id
order by d.cnt desc, ft.trip_id, ft.pickup_datetime;
