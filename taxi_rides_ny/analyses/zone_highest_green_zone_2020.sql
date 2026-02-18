-- Zone with highest revenue for Green taxis in 2020

SELECT
  pickup_zone,
  SUM(total_revenue) AS revenue_2020
FROM {{ref('fct_monthly_revenue_per_location')}}
WHERE taxi_type = 'green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY 1
ORDER BY revenue_2020 DESC
LIMIT 1;


-- Alternative approach using fct_trips directly (more granular, but less efficient):
SELECT
  pickup_zone,
  SUM(total_amount) AS revenue_2020
FROM {{ref('fct_trips')}}
WHERE lower(trim(taxi_type)) = 'green'
  AND pickup_datetime >= TIMESTAMP '2020-01-01'
  AND pickup_datetime <  TIMESTAMP '2021-01-01'
  AND pickup_zone IS NOT NULL
GROUP BY 1
ORDER BY revenue_2020 DESC
LIMIT 1;