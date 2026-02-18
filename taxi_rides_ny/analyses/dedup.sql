select
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    total_amount,
    count(*) as cnt
from {{ ref('int_trips_unioned') }}
group by 1,2,3,4,5,6
having count(*) > 1;