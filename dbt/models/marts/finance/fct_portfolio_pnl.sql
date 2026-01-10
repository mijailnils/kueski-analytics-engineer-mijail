{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'pnl']
    )
}}

-- Fact table: Portfolio P&L summary
-- Executive-level P&L view by vintage and risk segment
-- Used for CEO/CFO briefing and strategic decision making

with loan_financials as (
    select * from {{ ref('fct_loan_financials') }}
),

pnl_summary as (
    select
        -- Dimensions
        vintage_month,
        risk_segment,
        
        -- Volume metrics
        count(distinct loan_id) as total_loans,
        count(distinct user_id) as total_customers,
        sum(requested_amount) as gross_loan_volume,
        
        -- Revenue breakdown
        sum(interest_revenue) as interest_revenue,
        sum(fee_revenue) as fee_revenue,
        sum(penalty_revenue) as penalty_revenue,
        sum(revenue_total) as total_revenue,
        
        -- Cost breakdown
        sum(funding_cost) as funding_cost,  -- Charge-offs
        sum(cogs) as operational_cost,  -- COGS
        sum(cac) as acquisition_cost,  -- CAC
        
        -- Margin analysis
        sum(revenue_total - funding_cost) as financial_margin,
        sum(revenue_total - funding_cost - cogs) as contribution_margin,
        sum(revenue_total - funding_cost - cogs - cac) as net_profit,
        
        -- Per loan metrics
        avg(revenue_total) as revenue_per_loan,
        avg(funding_cost) as funding_cost_per_loan,
        avg(contribution_margin) as contribution_margin_per_loan,
        
        -- Efficiency ratios
        case 
            when sum(revenue_total) > 0 
            then sum(funding_cost) / sum(revenue_total) 
            else 0 
        end as funding_cost_ratio,
        
        case 
            when sum(revenue_total) > 0 
            then sum(cogs) / sum(revenue_total) 
            else 0 
        end as operational_cost_ratio,
        
        case 
            when sum(revenue_total) > 0 
            then sum(financial_margin) / sum(revenue_total) 
            else 0 
        end as financial_margin_pct,
        
        case 
            when sum(revenue_total) > 0 
            then sum(contribution_margin) / sum(revenue_total) 
            else 0 
        end as contribution_margin_pct,
        
        case 
            when sum(revenue_total) > 0 
            then (sum(revenue_total) - sum(funding_cost) - sum(cogs) - sum(cac)) / sum(revenue_total)
            else 0 
        end as net_profit_margin_pct,
        
        -- Risk metrics
        case 
            when sum(requested_amount) > 0 
            then sum(funding_cost) / sum(requested_amount) 
            else 0 
        end as loss_rate,
        
        sum(case when is_charged_off = 1 then 1 else 0 end) as charged_off_loans,
        sum(case when is_fully_paid = 1 then 1 else 0 end) as fully_paid_loans,
        sum(case when is_delinquent = 1 then 1 else 0 end) as delinquent_loans,
        
        -- Return metrics
        case
            when sum(cac) > 0
            then sum(contribution_margin) / sum(cac)
            else null
        end as ltv_to_cac_ratio
        
    from loan_financials
    group by 
        vintage_month,
        risk_segment
)

select * from pnl_summary
order by vintage_month, risk_segment