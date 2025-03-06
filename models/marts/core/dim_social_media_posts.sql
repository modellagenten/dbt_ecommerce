with social_media_posts as (
    select * from {{ ref('stg_social_media_posts') }}
),

posts_pro_platform as (
    select
        platform,
        count(POST_ID) AS C_POST_ID,
        avg(REACH) AS AVG_REACH,
        avg(ENGAGEMENT_RATE) AS AVG_ENGAGEMENT_RATE
    from {{ ref('stg_social_media_posts') }}
    group by platform
),

final as (
    select
        s.POST_ID,
s.PRODUCT_ID,
s.PLATFORM,
s.POST_DATE,
s.POST_TEXT,
s.LIKES,
s.SHARES,
s.COMMENTS,
s.REACH,
s.ENGAGEMENT_RATE,
s.CREATED_AT,
p.C_POST_ID,
 p.AVG_REACH,
p.AVG_ENGAGEMENT_RATE

    from social_media_posts s
    left join posts_pro_platform p on p.platform = s.platform
)

select * from final 