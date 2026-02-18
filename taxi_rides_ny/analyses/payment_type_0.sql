with payment_type_0 as (
    select
        count(*) as count_of_trips
    from {{ ref('fct_trips') }}
    where payment_type = 1
)

select * from payment_type_0

select count(*) from {{ ref('fct_trips') }}