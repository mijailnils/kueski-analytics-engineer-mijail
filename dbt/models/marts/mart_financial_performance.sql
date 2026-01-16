{{
    config(
        materialized='table',
        tags=['marts', 'financials']
    )
}}

-- Financial P&L metrics aggregated by vintage cohort

with loan_financials as (
    select * from {{ ref('int_loan_financials') }}
),

aggregated as (
    select
        vintage_month,
        vintage_year,
        risk_segment_customer,
        state_customer,
        
        -- Volume metrics
        count(distinct loan_id) as total_loans,
        count(distinct user_id) as total_customers,
        sum(loan_amount) as total_loan_amount,
        
        -- Revenue metrics
        sum(revenue) as revenue_total,
        sum(interest_revenue) as interest_total,
        sum(fee_revenue) as fees_total,
        sum(penalty_revenue) as penalties_total,
        
        -- Cost metrics
        sum(funding_cost) as funding_cost_total,
        sum(cac) as cac_total,
        sum(cogs) as cogs_total,
        
        -- Margin metrics
        sum(financial_margin) as financial_margin_total,
        sum(contribution_margin) as contribution_margin_total,
        sum(net_profit) as net_profit_total,
        
        -- Ratios
        sum(revenue) / nullif(count(distinct user_id), 0) as revenue_per_customer,
        sum(cac) / nullif(count(distinct user_id), 0) as cac_per_customer,
        sum(revenue) / nullif(sum(cac), 0) as ltv_cac_ratio,
        
        -- Unit economics
        sum(revenue) / nullif(count(distinct loan_id), 0) as revenue_per_loan,
        sum(cogs) / nullif(count(distinct loan_id), 0) as cogs_per_loan,
        sum(contribution_margin) / nullif(count(distinct loan_id), 0) as contribution_margin_per_loan,
        
        -- Loss rates
        sum(funding_cost) / nullif(sum(loan_amount), 0) as loss_rate,
        sum(case when is_delinquent = 1 then 1 else 0 end) / nullif(count(*), 0) as delinquency_rate,
        
        -- Principal recovery
        sum(principal_repaid) / nullif(sum(loan_amount), 0) as recovery_rate
        
    from loan_financials
    group by 
        vintage_month,
        vintage_year,
        risk_segment_customer,
        state_customer
)

select * from aggregated
