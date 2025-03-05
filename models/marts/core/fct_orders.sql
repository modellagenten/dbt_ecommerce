with transactions as (
    select * from {{ ref('stg_transactions') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

final as (
    select
        -- Keys
        t.transaction_id,
        t.customer_id,
        t.campaign_id,
        oi.order_item_id,
        oi.product_id,
        -- Dates
        date_trunc('day', to_date(t.transaction_date)) as order_date,
        date_trunc('month', to_date(t.transaction_date)) as order_month,
        date_trunc('year', to_date(t.transaction_date)) as order_year,
        -- Order details
        t.payment_method,
        t.shipping_method,
        t.status as order_status,
        -- Amounts
        oi.quantity as item_quantity,
        oi.unit_price as item_price,
        oi.total_price as item_total,
        t.total_amount as order_total,
        t.discount_amount as order_discount,
        t.final_amount as order_final_amount,
        -- Shipping
        t.shipping_postal_code,
        t.shipping_city,
        t.shipping_country,
        -- Timestamps
        t.created_at,
        t.updated_at
    from transactions t
    inner join order_items oi on t.transaction_id = oi.transaction_id
)

select * from final 