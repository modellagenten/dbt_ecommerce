with products as (
    select * from {{ ref('stg_products') }}
),

product_reviews as (
    select
        product_id,
        RATING,
        created_at,
        updated_at,
        DATEDIFF(hour, updated_at , created_at)
    from {{ ref('stg_products_reviews') }} pr
    -- comment
),

product_sales as (
    select
        product_id,
        stock_quantity
    from {{ ref('stg_products') }} ps
),

final as (
    select
        p.product_id,
        ps.stock_quantity
    from products p
    left join product_sales ps on p.product_id = ps.product_id
    left join product_reviews pr on p.product_id = pr.product_id
)

select * from final 