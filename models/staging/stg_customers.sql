with source as (
    select * from {{ source('ecommerce', 'customers') }}
),

renamed as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        birth_date,
        gender,
        street,
        postal_code,
        city,
        country,
        registration_date,
        customer_segment,
        newsletter_subscription,
        preferred_payment,
        created_at,
        updated_at
    from source
)

select * from renamed 