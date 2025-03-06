with product as (
    select product_name, product_id from {{ ref('stg_products') }}
),

daily_engagement as (
    select product_id, platform, POST_DATE, sum(likes) as SUM_LIKES, sum(SHARES) as SUM_SHARES, 
    sum(COMMENTS) AS SUM_COMMENTS, sum(REACH) AS SUM_REACH 
    from {{ ref('stg_social_media_posts') }}
    GROUP BY product_id, platform, POST_DATE
),

reach as (
    select product_id, sum(REACH) AS SUM_REACH, AVG(LIKES+COMMENTS+SHARES) AS AVG_INTERACTIONS
    from {{ ref('stg_social_media_posts') }}
    GROUP BY product_id
),



final as (
    select
    d.product_id,
    p.product_name,
    d.platform,
    d.POST_DATE,
    d.SUM_LIKES,d.SUM_SHARES, 
    d.SUM_COMMENTS, 
    d.SUM_REACH,
    r.SUM_REACH AS SUM_PRODUCT_REACH,
    r.AVG_INTERACTIONS
        
    from daily_engagement d
    left join reach r on r.PRODUCT_ID = d.PRODUCT_ID
    left join product p ON p.product_id = d.product_id
)

select * from final order by PRODUCT_ID,platform, POST_DATE