{{
    config(
        materialized='view',
        tags=['intermediate', 'q1_analysis']
    )
}}

-- Filter loans to Q1 2025 vintages only
-- Used for specific Q1 analysis and reporting

with loans_latest as (
    select * from {{ ref('int_loans_latest') }}
),

q1_vintages as (
    select
        -- IDs
        loan_id,
        user_id,
        
        -- Dates
        vintage_date,
        vintage_month,
        disbursed_date,
        
        -- Loan characteristics
        loan_amount,
        loan_term,
        interest_rate,
        delinquency_status,
        dpd_bucket,
        is_delinquent,
        
        -- Financial data
        funding_cost,
        cogs_total_cost,
        capital_balance,
        
        -- Snapshot info
        limit_month,
        limit_month_str
        
    from loans_latest
    where vintage_month in ('2025-01', '2025-02', '2025-03')
)

select * from q1_vintages
