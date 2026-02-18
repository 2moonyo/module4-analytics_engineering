select *
from {{ ref ('fct_trips') }}
where trip_id = '316633a5f213ffad798ed718c909a5ab'
order by total_amount desc, pickup_datetime;