{{
    config(
        materialized='view',
        tags=['marts', 'portfolio']
    )
}}

-- Fact table: Cohort-level performance metrics
-- Aggregates loans by vintage and risk segment
-- Provides core metrics for portfolio analysis

with loan_financials as (
    select * from {{ ref('fct_loan_financials') }}
),

cohort_metrics as (
    select
        -- Cohort dimensions
        vintage_month,
        risk_segment,
        
        -- Loan counts
        count(distinct loan_id) as loan_count,
        count(distinct user_id) as customer_count,
        
        -- Portfolio volumes
        sum(requested_amount) as total_requested,
        sum(funded_amount) as total_funded,
        avg(requested_amount) as avg_loan_size,
        
        -- Revenue metrics
        sum(revenue_total) as total_revenue,
        sum(interest_revenue) as total_interest_revenue,
        sum(fee_revenue) as total_fee_revenue,
        avg(revenue_total) as avg_revenue_per_loan,
        
        -- Cost metrics
        sum(funding_cost) as total_funding_cost,
        sum(cogs) as total_cogs,
        sum(cac) as total_cac,
        
        -- Margin metrics
        sum(financial_margin) as total_financial_margin,
        sum(contribution_margin) as total_contribution_margin,
        
        avg(financial_margin) as avg_financial_margin_per_loan,
        avg(contribution_margin) as avg_contribution_margin_per_loan,
        
        -- Margin percentages
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
        
        -- Loss metrics
        sum(case when is_charged_off = 1 then 1 else 0 end) as charged_off_count,
        
        case 
            when sum(requested_amount) > 0 
            then sum(funding_cost) / sum(requested_amount) 
            else 0 
        end as loss_rate,
        
        case 
            when count(distinct loan_id) > 0 
            then sum(case when is_charged_off = 1 then 1 else 0 end)::float / count(distinct loan_id)
            else 0
        end as charge_off_rate,
        
        -- Payment behavior
        sum(case when is_fully_paid = 1 then 1 else 0 end) as fully_paid_count,
        sum(case when is_current = 1 then 1 else 0 end) as current_count,
        sum(case when is_delinquent = 1 then 1 else 0 end) as delinquent_count,
        sum(principal_paid) as total_principal_collected,
        
        case 
            when count(distinct loan_id) > 0 
            then sum(case when is_fully_paid = 1 then 1 else 0 end)::float / count(distinct loan_id)
            else 0
        end as fully_paid_rate,
        
        case 
            when count(distinct loan_id) > 0 
            then sum(case when is_delinquent = 1 then 1 else 0 end)::float / count(distinct loan_id)
            else 0
        end as delinquency_rate,
        
        -- LTV vs CAC
        sum(contribution_margin) as total_ltv,
        avg(ltv_to_cac_ratio) as avg_ltv_to_cac_ratio,
        
        case
            when sum(cac) > 0
            then sum(contribution_margin) / sum(cac)
            else null
        end as cohort_ltv_to_cac_ratio
        
    from loan_financials
    group by 
        vintage_month,
        risk_segment
)

select * from cohort_metrics
order by vintage_month, risk_segment