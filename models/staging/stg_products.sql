with source as (
    select * from {{ source('ecommerce', 'products') }}
),

renamed as (
    select
        product_id,
        name as product_name,
        description as product_description,
        category,
        subcategory,
        base_price,
        current_price,
        stock_quantity,
        min_stock_threshold,
        supplier,
        weight_kg,
        is_active,
        created_at,
        updated_at
    from source
)

select * from renamed 