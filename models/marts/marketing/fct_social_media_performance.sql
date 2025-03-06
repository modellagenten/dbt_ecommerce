with social_media_metrics as (
    select product_id, SUM_PRODUCT_REACH, sum_likes, SUM_SHARES, SUM_COMMENTS 
    from {{ ref('fct_social_media_metrics') }} 
),

transactions as (
    select product_id, sum(item_quantity) AS sold_quantity from {{ ref('fct_orders') }} where ORDER_STATUS = 'COMPLETED'
    group by product_id
),

produktkategorien as (
select category, product_id from {{ ref('dim_products') }}
    
),

final as (
    select p.category,
        SUM(t.sold_quantity) AS QUANTITY,
        SUM(sm.sum_likes+sm.SUM_SHARES+sm.SUM_COMMENTS) AS ACTIVITIES,
        SUM(sm.SUM_PRODUCT_REACH) AS TOTAL_PRODUCTREACH

    from produktkategorien p
    JOIN transactions t on t.PRODUCT_ID = p.product_id
    JOIN social_media_metrics sm on sm.product_id = p.product_id
    GROUP BY p.category
    
)

select * from final 