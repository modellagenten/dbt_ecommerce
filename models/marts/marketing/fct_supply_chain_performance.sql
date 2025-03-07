with order_data as (
  select * from {{ ref('fct_orders')}}
),

supplier_metrics as (
    select * from {{ ref('stg_supplier_metrics')}}
),

metrik_product_delay_sale as (
    select o.product_id,
        o.order_status,
        avg(sm.DELIVERY_DELAY) as avg_delivery_delay,
        sum(o.item_quantity) as total_order_amount
    from order_data o
    left join supplier_metrics sm on o.product_id = sm.PRODUCT_ID
    group by o.product_id, o.order_status
)

select * from metrik_product_delay_sale