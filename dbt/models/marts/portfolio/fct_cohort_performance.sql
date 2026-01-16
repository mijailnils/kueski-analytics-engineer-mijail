{{
    config(
        materialized='view',
        tags=['marts', 'portfolio', 'cohort']
    )
}}

-- Cohort-level performance aggregations

with loan_performance as (
    select * from {{ ref('int_loan_performance') }}
),

cohort_metrics as (
    select
        vintage_month,
        risk_segment,
        state,
        
        -- Volume metrics
        count(distinct loan_id) as total_loans,
        count(distinct user_id) as total_customers,
        sum(loan_amount) as total_funded,
        
        -- Revenue metrics
        sum(revenue_total) as total_revenue,
        sum(interest_revenue) as total_interest,
        sum(fee_revenue) as total_fees,
        sum(penalty_revenue) as total_penalties,
        
        -- Cost metrics
        sum(funding_cost) as total_funding_cost,
        sum(cogs) as total_cogs,
        sum(cac) as total_cac,
        
        -- Margin metrics
        sum(financial_margin) as total_financial_margin,
        sum(contribution_margin) as total_contribution_margin,
        
        -- Performance metrics
        sum(case when is_delinquent_flag = 1 then 1 else 0 end) as delinquent_loans,
        sum(case when is_fully_paid = 1 then 1 else 0 end) as fully_paid_loans,
        sum(principal_paid) as total_principal_paid,
        
        -- Ratios
        avg(loss_rate) as avg_loss_rate,
        sum(revenue_total) / nullif(sum(cac), 0) as cohort_ltv_cac_ratio,
        sum(principal_paid) / nullif(sum(loan_amount), 0) as recovery_rate,
        sum(case when is_delinquent_flag = 1 then 1 else 0 end) / 
            nullif(count(*), 0) as delinquency_rate
        
    from loan_performance
    group by 
        vintage_month,
        risk_segment,
        state
)

select * from cohort_metrics
