{{
    config(
        materialized='table',
        schema='dbt_uzellbeck',
        alias=generate_date_suffix_table_name('transactions')
    )
}}

SELECT 
    *
FROM 
    {{ ref('stg_transactions') }}