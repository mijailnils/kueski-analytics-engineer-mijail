{{
    config(
        materialized='view',
        tags=['intermediate', 'performance']
    )
}}

-- Comprehensive loan-level performance metrics
-- Joins loans, repayments, and customer data
-- Calculates all financial metrics needed for analysis

with q1_loans as (
    select * from {{ ref('int_loans_q1_vintages') }}
),

repayments_agg as (
    select * from {{ ref('int_loan_repayments_agg') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

loan_performance as (
    select
        -- IDs and dimensions
        l.loan_id,
        l.user_id,
        l.vintage_month,
        l.disbursed_date,
        c.acquisition_date,
        c.acquisition_cost as cac,
        c.risk_band_production,
        c.risk_segment,
        c.channel,
        c.state,
        
        -- Loan characteristics
        l.term,
        l.interest_rate,
        l.requested_amount,
        l.funded_amount,
        l.delinquency_status,
        l.dpd_bucket,
        l.is_charged_off,
        
        -- Repayment data
        coalesce(r.total_revenue, 0) as revenue_total,
        coalesce(r.total_interest_revenue, 0) as interest_revenue,
        coalesce(r.total_fee_revenue, 0) as fee_revenue,
        coalesce(r.total_penalty_revenue, 0) as penalty_revenue,
        coalesce(r.total_principal_paid, 0) as principal_paid,
        coalesce(r.payment_count, 0) as payment_count,
        r.first_payment_date,
        r.last_payment_date,
        
        -- Costs
        l.charge_off as funding_cost,
        l.cogs_total_cost as cogs,
        l.capital_balance,
        
        -- Financial metrics
        coalesce(r.total_revenue, 0) - l.charge_off as financial_margin,
        coalesce(r.total_revenue, 0) - l.charge_off - l.cogs_total_cost as contribution_margin,
        
        -- Margin percentages
        case 
            when coalesce(r.total_revenue, 0) > 0 
            then (coalesce(r.total_revenue, 0) - l.charge_off) / coalesce(r.total_revenue, 0)
            else null 
        end as financial_margin_pct,
        
        case 
            when coalesce(r.total_revenue, 0) > 0 
            then (coalesce(r.total_revenue, 0) - l.charge_off - l.cogs_total_cost) / coalesce(r.total_revenue, 0)
            else null 
        end as contribution_margin_pct,
        
        -- Loss rate
        case
            when l.requested_amount > 0
            then l.charge_off / l.requested_amount
            else 0
        end as loss_rate,
        
        -- LTV/CAC ratio
        case
            when c.acquisition_cost > 0
            then (coalesce(r.total_revenue, 0) - l.charge_off - l.cogs_total_cost) / c.acquisition_cost
            else null
        end as ltv_to_cac_ratio,
        
        -- Status flags
        case when l.delinquency_status = 'Fully Paid' then 1 else 0 end as is_fully_paid,
        case when l.delinquency_status = 'Current' then 1 else 0 end as is_current,
        case when l.delinquency_status in ('Past due (1-29)', 'Past due (30-59)', 'Past due (60-89)', 'Past due (90-179)', 'Past due (180<)') then 1 else 0 end as is_delinquent,
        case when r.payment_count > 0 then 1 else 0 end as has_payments
        
    from q1_loans l
    left join repayments_agg r on l.loan_id = r.loan_id
    left join customers c on l.user_id = c.user_id
)

select * from loan_performance