with vendors as (
    select
        vendor_id,
        {{ get_vendor_data('vendor_id') }} as vendor_name
    from {{ ref('int_trips_unioned') }}
)

select DISTINCT(vendor_id), vendor_name 
from vendors
order by vendor_id