-- customer_analysis.sql
{{
    config(
        materialized="table",
        cluster_by=["c_mktsegment", "c_nationkey"],
        post_hook=["ALTER TABLE {{ this }} RESUME RECLUSTER"],
    )
}}
select
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
from {{ source("snowflake_sample", "customer") }}
