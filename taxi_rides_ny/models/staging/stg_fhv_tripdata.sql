with tripdata as (
  select *
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null 
),

renamed as (
  select
      'fhv' as taxi_type,
      
      -- identifiers
      cast(dispatching_base_num as string) as dispatching_base_num,
      cast(affiliated_base_number as string) as affiliated_base_number,
      cast(pulocationid as integer) as pickup_location_id,
      cast(dolocationid as integer) as dropoff_location_id,
      
      -- timestamps
      cast(pickup_datetime as timestamp) as pickup_datetime,
      cast(dropoff_datetime as timestamp) as dropoff_datetime,
      
      -- trip info
      cast(sr_flag as string) as sr_flag
      
  from tripdata
)

select * from renamed
