{{
    config(
        materialized='table',
        tags=['marts', 'vintage', 'cohort']
    )
}}

-- Vintage cohort curves showing cumulative performance over time

with loan_financials as (
    select * from {{ ref('int_loan_financials') }}
),

repayments as (
    select * from {{ ref('stg_repayments') }}
),

cohort_performance as (
    select
        l.vintage_month,
        l.risk_segment,
        
        -- Cohort age in months
        datediff('month', l.vintage_date, r.event_date) as months_since_origination,
        
        -- Cumulative metrics
        sum(r.principal_amount) as cumulative_principal_repaid,
        sum(r.revenue_total) as cumulative_revenue,
        count(distinct case when l.funding_cost > 0 then l.loan_id end) as cumulative_defaults,
        count(distinct l.loan_id) as total_loans_in_cohort,
        
        -- Cumulative ratios
        sum(r.principal_amount) / nullif(sum(l.loan_amount), 0) as cumulative_recovery_rate,
        count(distinct case when l.funding_cost > 0 then l.loan_id end) / 
            nullif(count(distinct l.loan_id), 0) as cumulative_default_rate,
        
        -- Financial margin evolution
        sum(r.revenue_total) - sum(l.funding_cost) as cumulative_financial_margin
        
    from loan_financials l
    left join repayments r on l.loan_id = r.loan_id
    where datediff('month', l.vintage_date, r.event_date) >= 0
    group by 
        l.vintage_month,
        l.risk_segment,
        datediff('month', l.vintage_date, r.event_date)
)

select * from cohort_performance
order by vintage_month, months_since_origination
