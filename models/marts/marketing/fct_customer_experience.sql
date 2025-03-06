with orders as
(
    select sum(case when order_status = 'COMPLETED' then item_quantity else 0 end) as item_quantity
    ,product_id 
from {{ ref('fct_orders') }}
-- comment
group by product_id
),
products as (
    select product_id,avg(satisfaction_score) as AVG_SCORE ,count(*) as SUM_SUPPORT_REQUESTS 
    from {{ ref('stg_customer_support')}}
    group by product_id

)
,
final as (
    select oi.product_id,
    oi.item_quantity,
    p.AVG_SCORE,
    p.SUM_SUPPORT_REQUESTS
     from orders oi
    left join products p on p.product_id = oi.product_id
)

select * from final