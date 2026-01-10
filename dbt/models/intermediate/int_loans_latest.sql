{{
    config(
        materialized='view',
        tags=['intermediate', 'loans']
    )
}}

-- Get the latest loan status for each loan_id
-- Loans table has monthly snapshots (limit_month), we want the most recent state

with loans_ranked as (
    select
        *,
        row_number() over (
            partition by loan_id 
            order by limit_month desc
        ) as rn
    from {{ ref('stg_loans') }}
),

latest_loans as (
    select
        loan_id,
        user_id,
        disbursed_date,
        vintage_month,
        limit_month as latest_observation_month,
        limit_month_str as latest_observation_month_str,
        term,
        interest_rate,
        requested_amount,
        funded_amount,
        cogs_total_cost,
        charge_off,
        capital_balance,
        delinquency_status,
        is_charged_off,
        dpd_bucket,
        original_schedule
    from loans_ranked
    where rn = 1
)

select * from latest_loans