with source as (
    select * from {{ source('ecommerce', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        customer_id,
        campaign_id,
        transaction_date,
        total_amount,
        discount_amount,
        final_amount,
        payment_method,
        status,
        shipping_method,
        shipping_address,
        shipping_city,
        shipping_postal_code,
        shipping_country,
        created_at,
        updated_at
    from source
)

select * from renamed 