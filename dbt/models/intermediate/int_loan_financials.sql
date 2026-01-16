{{
    config(
        materialized='view',
        tags=['intermediate', 'financials']
    )
}}

-- Combines loan data with repayment revenue to calculate financial margins
-- One row per loan with aggregated financial metrics

with loans as (
    select * from {{ ref('stg_loans') }}
),

repayments as (
    select
        loan_id,
        sum(revenue_total) as total_revenue,
        sum(principal_amount) as total_principal_repaid,
        sum(interest_amount) as total_interest,
        sum(fee_amount) as total_fees,
        sum(penalty_amount) as total_penalties
    from {{ ref('stg_repayments') }}
    group by loan_id
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        l.loan_id,
        l.user_id,
        l.vintage_date,
        l.vintage_month,
        l.vintage_year,
        l.loan_amount,
        l.loan_term,
        l.interest_rate,
        l.requested_amount,
        l.delinquency_status,
        l.is_delinquent,
        l.dpd_bucket,
        l.capital_balance,
        
        -- Revenue components
        coalesce(r.total_revenue, 0) as revenue,
        coalesce(r.total_interest, 0) as interest_revenue,
        coalesce(r.total_fees, 0) as fee_revenue,
        coalesce(r.total_penalties, 0) as penalty_revenue,
        
        -- Funding cost (charge-offs)
        l.funding_cost,
        
        -- CAC (customer acquisition cost)
        coalesce(c.acquisition_cost, 0) as cac,
        
        -- COGS REAL (from loans data - includes all direct costs for this loan)
        coalesce(l.cogs_total_cost, 0) as cogs,
        
        -- Financial margin = Revenue - Funding cost
        {{ calculate_financial_margin(
            'coalesce(r.total_revenue, 0)',
            'l.funding_cost'
        ) }} as financial_margin,
        
        -- Contribution margin = Revenue - Funding cost - COGS
        {{ calculate_contribution_margin(
            'coalesce(r.total_revenue, 0)',
            'l.funding_cost',
            'coalesce(l.cogs_total_cost, 0)'
        ) }} as contribution_margin,
        
        -- Principal repayment
        coalesce(r.total_principal_repaid, 0) as principal_repaid,
        
        -- Net profit = Contribution margin
        {{ calculate_contribution_margin(
            'coalesce(r.total_revenue, 0)',
            'l.funding_cost',
            'coalesce(l.cogs_total_cost, 0)'
        ) }} as net_profit,
        
        -- Recovery rate
        coalesce(r.total_principal_repaid, 0) / nullif(l.loan_amount, 0) as recovery_rate,

        -- customer
        c.risk_segment,           
        c.risk_band_production,   
        c.channel,                
        c.state,                 
        c.city                 
        
    from loans l
    left join repayments r on l.loan_id = r.loan_id
    left join customers c on l.user_id = c.user_id
)

select * from joined