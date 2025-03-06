with products as (
    select * from {{ ref('stg_products') }}
),

product_reviews as (
    select
        product_id,
        COUNT(1) as cnt_rating,
        avg(RATING) as avg_rating,
        avg(HELPFUL_VOTES) as avg_helpful_votes,
        SUM(num_verified_purchase) as num_verified_purchase
    from {{ ref('stg_products_reviews') }} pr
    group by product_id
),

product_sales as (
    select
        p.product_id,
        count(distinct t.transaction_id) as number_of_orders,
        sum(oi.quantity) as total_quantity_sold,
        sum(oi.total_price) as total_revenue,
        avg(sm.STOCK_LEVEL) as avg_stock_level,
        avg(sm.QUALITY_SCORE) as avg_quality_score,
        avg(sm.DELIVERY_DELAY) as avg_delivery_delay
    from {{ ref('stg_products') }} p
    left join {{ ref('stg_order_items') }} oi on p.product_id = oi.product_id
    left join {{ ref('stg_transactions') }} t on oi.transaction_id = t.transaction_id
    left join {{ ref('stg_supplier_metrics') }} sm on p.product_id = sm.product_id
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
        p.base_price,
        p.current_price,
        p.stock_quantity,
        p.min_stock_threshold,
        p.supplier,
        p.weight_kg,
        p.is_active,
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
        ps.avg_stock_level,
        ps.avg_quality_score,
        ps.avg_delivery_delay,
        -- Timestamps
        p.created_at,
        p.updated_at,
        pr.avg_rating,
        pr.cnt_rating,
        pr.avg_helpful_votes,
        pr.num_verified_purchase
    from products p
    left join product_sales ps on p.product_id = ps.product_id
    left join product_reviews pr on p.product_id = pr.product_id
)

select * from final 