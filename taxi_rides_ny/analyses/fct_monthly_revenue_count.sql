-- Q3: Count of records in fct_monthly_zone_revenue

SELECT
  revenue_month,
  pickup_location_id,
  pickup_zone,
  pickup_borough,
  taxi_type,
  COUNT(*) AS n
FROM {{ref('fct_monthly_revenue_per_location')}}
GROUP BY 1,2,3,4,5
HAVING COUNT(*) > 1
ORDER BY n DESC;