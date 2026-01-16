{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Get the latest observation for each loan (most recent limit_month)

with loans as (
    select * from {{ ref('stg_loans') }}
),

latest_snapshot as (
    select
        loan_id,
        user_id,
        vintage_date,
        vintage_month,
        loan_amount,
        loan_term,
        interest_rate,
        cogs_total_cost,
        funding_cost,
        is_delinquent,
        delinquency_status,
        dpd_bucket,
        capital_balance,
        limit_month,
        strftime(limit_month, '%Y-%m') as limit_month_str,
        disbursed_date
    from loans
)

select * from latest_snapshot
