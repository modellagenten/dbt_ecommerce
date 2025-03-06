with source as (
    select * from {{ source('ecommerce', 'social_media_posts') }}
),

renamed as (
    select
        POST_ID,
PRODUCT_ID,
PLATFORM,
TO_TIMESTAMP(POST_DATE) AS POST_DATE,
TRIM(POST_TEXT) AS POST_TEXT,
LIKES,
SHARES,
COMMENTS,
REACH,
ENGAGEMENT_RATE,
TO_TIMESTAMP(CREATED_AT) AS CREATED_AT
    from source
)

select * from renamed 