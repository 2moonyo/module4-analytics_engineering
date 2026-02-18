with payment_type_lookup as (  
    select * 
    from {{ ref('payment_type_lookup') }}
),

renamed as (
    select
        payment_type as payment_type_id,
        description as payment_type_description

    from payment_type_lookup   

)

SELECT * FROM renamed
