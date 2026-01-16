{{
    config(
        materialized='view',
        tags=['marts', 'finance', 'pnl']
    )
}}

-- Portfolio-level P&L statement

with loan_performance as (
    select * from {{ ref('int_loan_performance') }}
),

portfolio_pnl as (
    select
        vintage_month,
        
        -- Volume
        count(distinct loan_id) as loan_count,
        count(distinct user_id) as customer_count,
        sum(loan_amount) as gross_loan_volume,
        
        -- Revenue
        sum(revenue_total) as total_revenue,
        sum(interest_revenue) as interest_income,
        sum(fee_revenue) as fee_income,
        sum(penalty_revenue) as penalty_income,
        
        -- Direct costs
        sum(funding_cost) as funding_losses,
        sum(cogs) as operating_costs,
        
        -- Gross profit
        sum(financial_margin) as gross_profit,
        
        -- Customer acquisition
        sum(cac) as customer_acquisition_cost,
        
        -- Net profit
        sum(contribution_margin) as net_profit,
        
        -- Margins
        sum(financial_margin) / nullif(sum(revenue_total), 0) as gross_margin_pct,
        sum(contribution_margin) / nullif(sum(revenue_total), 0) as net_margin_pct,
        
        -- Unit economics
        sum(revenue_total) / nullif(count(distinct loan_id), 0) as revenue_per_loan,
        sum(contribution_margin) / nullif(count(distinct loan_id), 0) as profit_per_loan,
        sum(cac) / nullif(count(distinct user_id), 0) as cac_per_customer,
        
        -- Loss metrics
        sum(funding_cost) / nullif(sum(loan_amount), 0) as portfolio_loss_rate,
        sum(case when is_delinquent_flag = 1 then 1 else 0 end) / 
            nullif(count(*), 0) as portfolio_delinquency_rate
        
    from loan_performance
    group by vintage_month
)

select * from portfolio_pnl
order by vintage_month
