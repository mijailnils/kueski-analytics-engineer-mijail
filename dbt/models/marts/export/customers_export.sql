{{
    config(
        materialized='view',
        tags=['marts', 'export']
    )
}}

-- Customer export with recurrency flag

with customers as (
    select * from {{ ref('stg_customers') }}
),

loan_counts as (
    select
        user_id,
        count(distinct loan_id) as total_loans
    from {{ ref('stg_loans') }}
    group by user_id
),

customers_enriched as (
    select
        c.*,
        coalesce(lc.total_loans, 0) as total_loans,
        case when coalesce(lc.total_loans, 0) > 1 then 1 else 0 end as flg_recurrent_customer
    from customers c
    left join loan_counts lc on c.user_id = lc.user_id
)

select * from customers_enriched
