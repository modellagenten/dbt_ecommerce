with source as (
    select * from {{ source('ecommerce', 'marketing_campaigns') }}
),

renamed as (
    select
        campaign_id,
        name as campaign_name,
        description as campaign_description,
        type as campaign_type,
        primary_channel,
        secondary_channels,
        start_date,
        end_date,
        budget,
        target_audience,
        target_impressions,
        target_clicks,
        target_conversions,
        discount_percentage,
        is_active,
        created_at,
        updated_at
    from source
)

select * from renamed 