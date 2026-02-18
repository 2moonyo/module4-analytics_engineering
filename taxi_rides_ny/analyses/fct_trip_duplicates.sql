select trip_id, count(*) as cnt
from {{ ref('fct_trips') }}
group by 1
having count(*) > 1
order by cnt desc
limit 50;