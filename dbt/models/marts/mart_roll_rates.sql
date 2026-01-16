{{
    config(
        materialized='table',
        tags=['marts', 'delinquency', 'roll_rates']
    )
}}

-- Roll rate analysis: tracks how loans move between DPD buckets

with loan_financials as (
    select * from {{ ref('int_loan_financials') }}
),

roll_rate_analysis as (
    select
        vintage_month,
        risk_segment_customer,
        
        -- Current state distribution
        count(distinct case when dpd_bucket = '0_Current' then loan_id end) as current_count,
        count(distinct case when dpd_bucket = 'Delinquent' then loan_id end) as delinquent_count,
        count(distinct case when dpd_bucket = 'Unknown' then loan_id end) as unknown_count,
        
        count(distinct loan_id) as total_loans,
        
        -- Delinquency dynamics
        sum(case when is_delinquent = 1 then 1 else 0 end) / 
            nullif(count(*), 0) as current_delinquency_rate,
            
        -- Loss given default proxy
        sum(funding_cost) / nullif(sum(loan_amount), 0) as loss_rate
        
    from loan_financials
    group by vintage_month, risk_segment_customer
)

select * from roll_rate_analysis
