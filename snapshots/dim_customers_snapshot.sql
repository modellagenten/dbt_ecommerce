{% snapshot dim_customers_snapshot %}

    {{
        config(
            target_schema="snapshots",
            strategy="timestamp",
            unique_key="customer_id",
            updated_at="updated_at",
        )
    }}

    select customer_id, email, customer_status, customer_segment, updated_at
    from {{ ref("dim_customers") }}

{% endsnapshot %}
