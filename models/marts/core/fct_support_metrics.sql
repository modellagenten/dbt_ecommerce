with customer_support as (
    select * from {{ ref('stg_customer_support') }}
),

final as (
    select PRODUCT_ID ,
    TICKET_TYPE ,
    PRIORITY ,
    to_date(TICKET_DATE) as DATUM,
    sum(case when status = 'OPEN' then 1 else 0 end) as CNT_OPEN_TICKETS,
    avg(RESOLUTION_TIME) as AVG_RESOLUTION_TIME
    from customer_support
    group by 1,2,3,4 --to_date(TICKET_DATE)
)

select * from final
