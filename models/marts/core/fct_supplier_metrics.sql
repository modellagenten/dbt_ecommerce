with supplier_metrics as (
    select * from {{ ref('stg_supplier_metrics') }}
),

final as (
    select
        -- Keys
        sm.product_id,
        sm.SUPPLIER_ID,
        -- Dates
        to_date(sm.DELIVERY_DATE) as delivery_date_,
        -- amounts
        sum(sm.QUANTITY) as total_quantity,
        avg(sm.QUANTITY) as avg_quantity,
        sum(sm.UNIT_PRICE) as total_unit_price,
        avg(sm.QUALITY_SCORE) as avg_quality_score,
        max(sm.QUALITY_SCORE) as max_quality_score,
        sum(sm.STOCK_LEVEL) as total_stock_level
    from supplier_metrics sm
    group by sm.product_id, sm.SUPPLIER_ID, delivery_date_
)

select * from final 