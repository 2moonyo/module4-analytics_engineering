-- Zone with highest revenue for Green taxis in 2020

SELECT COUNT(*) AS total_trips
FROM {{ref('fct_trips')}}
WHERE lower(trim(taxi_type)) = 'green'
  AND pickup_datetime >= TIMESTAMP '2019-10-01'
  AND pickup_datetime <  TIMESTAMP '2019-11-01';