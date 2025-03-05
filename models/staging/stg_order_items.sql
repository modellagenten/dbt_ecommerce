with source as (
    select * from {{ source('ecommerce', 'order_items') }}
),

renamed as (
    select
        order_item_id,
        transaction_id,
        product_id,
        quantity,
        unit_price,
        total_price,
        created_at
    from source
)

select * from renamed 