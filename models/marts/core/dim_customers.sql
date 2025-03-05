with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select
        customer_id,
        count(*) as lifetime_orders,
        sum(final_amount) as lifetime_revenue,
        min(transaction_date) as first_order_date,
        max(transaction_date) as last_order_date
    from {{ ref('stg_transactions') }}
    where status = 'COMPLETED'
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.birth_date,
        c.gender,
        -- Address
        c.street,
        c.postal_code,
        c.city,
        c.country,
        -- Customer attributes
        c.registration_date,
        c.customer_segment,
        c.newsletter_subscription,
        c.preferred_payment,
        -- Order metrics
        coalesce(co.lifetime_orders, 0) as lifetime_orders,
        coalesce(co.lifetime_revenue, 0) as lifetime_revenue,
        co.first_order_date,
        co.last_order_date,
        -- Timestamps
        c.created_at,
        c.updated_at,
        -- Derived fields
        case 
            when co.last_order_date is null then 'Never Ordered'
            when datediff('day', co.last_order_date, current_date) > 365 then 'Churned'
            when datediff('day', co.last_order_date, current_date) > 180 then 'At Risk'
            else 'Active'
        end as customer_status
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from final 