{{
    config(
        materialized='view',
        tags=['intermediate', 'loans']
    )
}}

-- Q1 2025 loan vintages with cohort identification

with latest_loans as (
    select * from {{ ref('int_loans_latest') }}
),

q1_vintages as (
    select
        -- IDs
        loan_id,
        user_id,
        
        -- Dates - CONVERTIR vintage_month a DATE
        CAST(strptime(strftime(disbursed_date, '%Y-%m') || '-01', '%Y-%m-%d') AS DATE) as vintage_month,
        disbursed_date,
        
        -- Loan characteristics
        term,
        interest_rate,
        requested_amount,
        funded_amount,
        delinquency_status,
        dpd_bucket,
        
        -- Financial data
        charge_off,
        cogs_total_cost,
        capital_balance,
        
        -- Flags
        is_charged_off

    from latest_loans
    where strftime(disbursed_date, '%Y-%m') in ('2025-01', '2025-02', '2025-03')
)

select * from q1_vintages
