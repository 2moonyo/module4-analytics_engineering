{% macro void_flag(model_name) %}

with identity_group as (
    select
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,

        max(case when fare_amount > 0 then 1 else 0 end) as has_positive,
        max(case when fare_amount < 0 then 1 else 0 end) as has_negative

    from {{ model_name }}
    group by 1,2,3,4,5,6,7
)

select
    b.*,
    (g.has_positive = 1 and g.has_negative = 1) as is_void
from {{ model_name }} b
left join identity_group g
  on  b.vendor_id = g.vendor_id
  and b.pickup_datetime = g.pickup_datetime
  and b.dropoff_datetime = g.dropoff_datetime
  and b.pickup_location_id = g.pickup_location_id
  and b.dropoff_location_id = g.dropoff_location_id
  and b.passenger_count = g.passenger_count
  and b.trip_distance = g.trip_distance

{% endmacro %}
