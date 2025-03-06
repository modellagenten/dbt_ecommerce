with source as (
    select * from {{ source('ecommerce', 'customer_support') }}
),

stage_table as (
    select 
    TICKET_ID,
    CUSTOMER_ID,
    PRODUCT_ID ,
    to_timestamp(TICKET_DATE) as TICKET_DATE ,
    TICKET_TYPE ,
    PRIORITY ,
    STATUS ,
    RESOLUTION_TIME ,
    SATISFACTION_SCORE ,
    to_timestamp(CREATED_AT) as CREATED_AT
    from source
)

select * from stage_table