{{
    config(
        materialized='view',
        tags=['staging', 'loans']
    )
}}

-- Staging model for loan data
-- One row per loan-month snapshot with parsed schedule

with source as (
    select * from read_csv_auto('C:/Users/mijai/kueski-analytics-engineer-mijail/data/raw/AE_challenge_loans.csv')
),

cleaned as (
    select
        -- IDs
        loan_id,
        user_id,
        
        -- Dates
        cast(disbursed_date as date) as disbursed_date,
        strftime(cast(disbursed_date as date), '%Y-%m') as vintage_month,
        cast(limit_month as date) as limit_month,
        strftime(cast(limit_month as date), '%Y-%m') as limit_month_str,
        
        -- Loan terms
        term,
        interestrate as interest_rate,
        
        -- Amounts
        requested_amount,
        founded_amount as funded_amount,
        cogs_total_cost,
        charge_off,
        capital_balance,
        
        -- Status
        delinquency_status,
        
        -- Derived flags
        case when charge_off > 0 then true else false end as is_charged_off,
        
        -- DPD bucket mapping
        case 
            when delinquency_status = 'Fully Paid' then '0_Fully_Paid'
            when delinquency_status = 'Current' then '0_Current'
            when delinquency_status = 'Past due (1-29)' then '1_DPD_1_29'
            when delinquency_status = 'Past due (30-59)' then '2_DPD_30_59'
            when delinquency_status = 'Past due (60-89)' then '3_DPD_60_89'
            when delinquency_status = 'Past due (90-179)' then '4_DPD_90_179'
            when delinquency_status = 'Past due (180<)' then '5_DPD_180_plus'
            when delinquency_status = 'Sold' then '6_Sold'
            else '7_Unknown'
        end as dpd_bucket,
        
        -- Keep original schedule
        original_schedule
        
    from source
)

select * from cleaned