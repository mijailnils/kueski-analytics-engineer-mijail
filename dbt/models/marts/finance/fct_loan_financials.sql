{{
    config(
        materialized='table',
        tags=['marts', 'finance']
    )
}}

-- Fact table: Loan-level financials
-- Core financial table with all revenue, costs, and margins per loan
-- Ready for aggregation and analysis in Tableau

select
    -- Primary key
    loan_id,
    
    -- Foreign keys / dimensions
    user_id,
    vintage_month,
    risk_segment,
    risk_band_production,
    channel,
    state,
    dpd_bucket,
    delinquency_status,
    
    -- Dates
    disbursed_date,
    acquisition_date,
    first_payment_date,
    last_payment_date,
    
    -- Loan characteristics
    term,
    interest_rate,
    requested_amount,
    funded_amount,
    capital_balance,
    
    -- Revenue (from repayments)
    revenue_total,
    interest_revenue,
    fee_revenue,
    penalty_revenue,
    principal_paid,
    payment_count,
    
    -- Costs
    funding_cost,  -- charge-off amount
    cogs,
    cac,
    
    -- Margins
    financial_margin,  -- Revenue - Funding cost
    contribution_margin,  -- Revenue - Funding cost - COGS
    financial_margin_pct,
    contribution_margin_pct,
    
    -- Performance metrics
    loss_rate,
    ltv_to_cac_ratio,
    
    -- Flags
    is_fully_paid,
    is_current,
    is_delinquent,
    is_charged_off,
    has_payments

from {{ ref('int_loan_performance') }}