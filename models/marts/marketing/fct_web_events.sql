with web_events as (
    select * from {{ ref('stg_web_events') }}
),

session_metrics as (
    select
        session_id,
        count(*) as events_in_session,
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        sum(case when event_type = 'product_view' then 1 else 0 end) as product_views,
        sum(case when event_type = 'add_to_cart' then 1 else 0 end) as add_to_carts,
        sum(case when event_type = 'checkout_start' then 1 else 0 end) as checkout_starts,
        sum(case when event_type = 'checkout_complete' then 1 else 0 end) as checkout_completes
    from web_events
    group by session_id
),

final as (
    select
        -- Keys
        we.event_id,
        we.customer_id,
        we.session_id,
        we.product_id,
        -- Event details
        we.event_timestamp,
        date_trunc('day', we.event_timestamp) as event_date,
        date_trunc('month', we.event_timestamp) as event_month,
        we.event_type,
        we.device_type,
        we.page_url,
        we.referrer,
        -- Session metrics
        sm.events_in_session,
        sm.session_start,
        sm.session_end,
        datediff('minute', sm.session_start, sm.session_end) as session_duration_minutes,
        sm.product_views,
        sm.add_to_carts,
        sm.checkout_starts,
        sm.checkout_completes,
        -- Device info
        we.user_agent,
        we.ip_address,
        -- Timestamps
        we.created_at
    from web_events we
    left join session_metrics sm on we.session_id = sm.session_id
)

select * from final 