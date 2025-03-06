with source as (
    select * from {{ source('ecommerce', 'supplier_metrics') }}
),

renamed as (
    select
        SUPPLIER_ID,
        PRODUCT_ID,
        TO_TIMESTAMP(DELIVERY_DATE) as DELIVERY_DATE,
        TO_TIMESTAMP(ORDER_DATE) as ORDER_DATE,
        QUANTITY,
        UNIT_PRICE,
        QUALITY_SCORE,
        DELIVERY_DELAY,
        STOCK_LEVEL,
        TO_TIMESTAMP(CREATED_AT)  as CREATED_AT
    from source
)

select * from renamed 