with source as (
    select * from {{ source('ecommerce', 'web_events') }}
),

renamed as (
    select
        event_id,
        timestamp as event_timestamp,
        event_type,
        customer_id,
        session_id,
        device_type,
        user_agent,
        ip_address,
        page_url,
        referrer,
        product_id,
        created_at
    from source
)

select * from renamed 