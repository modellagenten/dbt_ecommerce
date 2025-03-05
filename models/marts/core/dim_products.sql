with products as (
    select * from {{ ref('stg_products') }}
),

product_sales as (
    select
        p.product_id,
        count(distinct t.transaction_id) as number_of_orders,
        sum(oi.quantity) as total_quantity_sold,
        sum(oi.total_price) as total_revenue
    from {{ ref('stg_products') }} p
    left join {{ ref('stg_order_items') }} oi on p.product_id = oi.product_id
    left join {{ ref('stg_transactions') }} t on oi.transaction_id = t.transaction_id
    where t.status = 'COMPLETED'
    group by p.product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.product_description,
        p.category,
        p.subcategory,
        p.brand,
        p.base_price,
        p.current_price,
        p.stock_quantity,
        p.min_stock_threshold,
        p.supplier,
        p.weight_kg,
        p.is_active,
        p.sku,
        -- Sales metrics
        coalesce(ps.number_of_orders, 0) as number_of_orders,
        coalesce(ps.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(ps.total_revenue, 0) as total_revenue,
        -- Inventory status
        case
            when p.stock_quantity = 0 then 'Out of Stock'
            when p.stock_quantity <= p.min_stock_threshold then 'Low Stock'
            else 'In Stock'
        end as stock_status,
        -- Price tier
        case
            when p.current_price < 50 then 'Budget'
            when p.current_price < 100 then 'Standard'
            when p.current_price < 200 then 'Premium'
            else 'Luxury'
        end as price_tier,
        -- Timestamps
        p.created_at,
        p.updated_at
    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from final 