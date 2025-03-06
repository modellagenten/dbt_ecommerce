with source as (
    select * from {{ source('ecommerce', 'product_reviews') }}
),

renamed as (
    select
        REVIEW_ID,
        PRODUCT_ID,
        CUSTOMER_ID,
        RATING,
        TITLE,
        CONTENT,
        HELPFUL_VOTES,
        VERIFIED_PURCHASE,
        case when VERIFIED_PURCHASE = true then 1
        else 0 
        ENd as num_verified_purchase,
        TO_TIMESTAMP(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS') as CREATED_AT,
        TO_TIMESTAMP(UPDATED_AT, 'YYYY-MM-DD HH24:MI:SS') as UPDATED_AT
    from source
)

select * from renamed 