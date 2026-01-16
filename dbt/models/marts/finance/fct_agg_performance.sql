{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'aggregated']
    )
}}

-- Aggregated performance metrics by channel, risk segment, month, and customer type

with loan_financials as (
    select * from {{ ref('int_loan_financials') }}
),

aggregated as (
    select
        vintage_month as month,
        channel_customer,
        risk_segment_customer,
        flg_recurrent_customer,
        
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
        
        -- Unit economics
        sum(revenue) / nullif(count(distinct loan_id), 0) as revenue_per_loan,
        sum(cac) / nullif(count(distinct user_id), 0) as cac_per_customer,
        sum(contribution_margin) / nullif(count(distinct loan_id), 0) as contribution_margin_per_loan,
        sum(net_profit) / nullif(count(distinct loan_id), 0) as net_profit_per_loan,
        
        -- Ratios
        sum(revenue) / nullif(sum(cac), 0) as ltv_cac_ratio,
        sum(funding_cost) / nullif(sum(loan_amount), 0) as loss_rate,
        sum(case when is_delinquent = 1 then 1 else 0 end) / nullif(count(*), 0) as delinquency_rate,
        sum(principal_repaid) / nullif(sum(loan_amount), 0) as recovery_rate,
        
        -- Loan sequence metrics
        sum(case when flg_first_loan_customer = 1 then 1 else 0 end) as first_loans_count,
        sum(case when flg_first_loan_customer = 0 then 1 else 0 end) as repeat_loans_count
        
    from loan_financials
    group by 
        vintage_month,
        channel_customer,
        risk_segment_customer,
        flg_recurrent_customer
)

select * from aggregated
order by month, channel_customer, risk_segment_customer, flg_recurrent_customer
