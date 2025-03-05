with campaigns as (
    select * from {{ ref('stg_marketing_campaigns') }}
),

campaign_orders as (
    select
        campaign_id,
        count(distinct transaction_id) as number_of_orders,
        count(distinct customer_id) as number_of_customers,
        sum(final_amount) as total_revenue,
        sum(discount_amount) as total_discount_amount
    from {{ ref('stg_transactions') }}
    where status = 'COMPLETED'
    and campaign_id is not null
    group by campaign_id
),

campaign_events as (
    select
        c.campaign_id,
        count(distinct we.session_id) as number_of_sessions,
        count(*) as number_of_events,
        count(distinct we.customer_id) as number_of_visitors,
        sum(case when we.event_type = 'product_view' then 1 else 0 end) as product_views,
        sum(case when we.event_type = 'add_to_cart' then 1 else 0 end) as add_to_carts,
        sum(case when we.event_type = 'checkout_start' then 1 else 0 end) as checkout_starts,
        sum(case when we.event_type = 'checkout_complete' then 1 else 0 end) as checkout_completes
    from {{ ref('stg_marketing_campaigns') }} c
    left join {{ ref('stg_web_events') }} we 
        on we.event_timestamp between c.start_date and c.end_date
    group by c.campaign_id
),

final as (
    select
        -- Campaign details
        c.campaign_id,
        c.campaign_name,
        c.campaign_type,
        c.primary_channel,
        c.secondary_channels,
        c.start_date,
        c.end_date,
        c.budget,
        c.target_audience,
        c.discount_percentage,
        -- Order metrics
        coalesce(co.number_of_orders, 0) as number_of_orders,
        coalesce(co.number_of_customers, 0) as number_of_customers,
        coalesce(co.total_revenue, 0) as total_revenue,
        coalesce(co.total_discount_amount, 0) as total_discount_amount,
        -- Web metrics
        coalesce(ce.number_of_sessions, 0) as number_of_sessions,
        coalesce(ce.number_of_events, 0) as number_of_events,
        coalesce(ce.number_of_visitors, 0) as number_of_visitors,
        coalesce(ce.product_views, 0) as product_views,
        coalesce(ce.add_to_carts, 0) as add_to_carts,
        coalesce(ce.checkout_starts, 0) as checkout_starts,
        coalesce(ce.checkout_completes, 0) as checkout_completes,
        -- Calculated metrics
        case
            when ce.product_views > 0 then 
                round(ce.add_to_carts::float / ce.product_views * 100, 2)
            else 0
        end as add_to_cart_rate,
        case
            when ce.checkout_starts > 0 then
                round(ce.checkout_completes::float / ce.checkout_starts * 100, 2)
            else 0
        end as checkout_completion_rate,
        case
            when c.budget > 0 then
                round(coalesce(co.total_revenue, 0) / c.budget, 2)
            else 0
        end as roi,
        -- Status
        c.is_active,
        -- Timestamps
        c.created_at,
        c.updated_at
    from campaigns c
    left join campaign_orders co on c.campaign_id = co.campaign_id
    left join campaign_events ce on c.campaign_id = ce.campaign_id
)

select * from final 