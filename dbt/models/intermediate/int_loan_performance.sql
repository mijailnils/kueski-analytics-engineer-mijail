{{
    config(
        materialized='view',
        tags=['intermediate', 'performance']
    )
}}

-- Comprehensive loan-level performance metrics

with q1_loans as (
    select * from {{ ref('int_loans_q1_vintages') }}
),

repayments_agg as (
    select * from {{ ref('int_loan_repayments_agg') }}
),

customer_metrics as (
    select * from {{ ref('int_customer_loan_metrics') }}
),

loans_with_sequence as (
    select
        l.*,
        ROW_NUMBER() OVER (PARTITION BY l.user_id ORDER BY l.disbursed_date ASC) as loan_sequence_number
    from q1_loans l
),

loan_performance as (
    select
        -- IDs and dimensions
        l.loan_id,
        l.user_id,
        CAST(l.vintage_month AS VARCHAR) as vintage_month,
        
        -- NUEVAS METRICAS
        cm.total_loans as customer_total_loans,
        cm.cac_per_loan as customer_cac_per_loan,
        l.loan_sequence_number,
        
        -- Dates
        l.disbursed_date,
        cm.acquisition_date,
        
        -- Customer attributes
        cm.risk_band_production,
        cm.risk_segment,
        cm.channel,
        cm.state,
        
        -- Loan characteristics
        l.term,
        l.interest_rate,
        l.requested_amount,
        l.funded_amount,
        l.delinquency_status,
        l.dpd_bucket,
        l.is_charged_off,
        
        -- Revenue metrics (nombres correctos)
        COALESCE(r.total_revenue, 0) as revenue_total,
        COALESCE(r.total_interest_revenue, 0) as interest_revenue,
        COALESCE(r.total_fee_revenue, 0) as fee_revenue,
        COALESCE(r.total_penalty_revenue, 0) as penalty_revenue,
        COALESCE(r.total_principal_paid, 0) as principal_paid,
        COALESCE(r.payment_count, 0) as payment_count,
        r.first_payment_date,
        r.last_payment_date,
        
        -- Cost metrics
        COALESCE(l.charge_off, 0) as funding_cost,
        COALESCE(l.cogs_total_cost, 0) as cogs,
        
        -- CAC: Solo al préstamo #1
        CASE 
            WHEN l.loan_sequence_number = 1 THEN cm.cac_total
            ELSE 0 
        END as cac,
        
        l.capital_balance,
        
        -- Margins
        COALESCE(r.total_revenue, 0) - COALESCE(l.charge_off, 0) as financial_margin,
        COALESCE(r.total_revenue, 0) - COALESCE(l.charge_off, 0) - COALESCE(l.cogs_total_cost, 0) as contribution_margin,
        
        CASE 
            WHEN COALESCE(r.total_revenue, 0) > 0 
            THEN (COALESCE(r.total_revenue, 0) - COALESCE(l.charge_off, 0)) / COALESCE(r.total_revenue, 0)
            ELSE 0 
        END as financial_margin_pct,
        
        CASE 
            WHEN COALESCE(r.total_revenue, 0) > 0 
            THEN (COALESCE(r.total_revenue, 0) - COALESCE(l.charge_off, 0) - COALESCE(l.cogs_total_cost, 0)) / COALESCE(r.total_revenue, 0)
            ELSE 0 
        END as contribution_margin_pct,
        
        -- Loss rate
        CASE 
            WHEN l.funded_amount > 0 
            THEN COALESCE(l.charge_off, 0) / l.funded_amount 
            ELSE 0 
        END as loss_rate,
        
        -- LTV/CAC ratio
        CASE 
            WHEN l.loan_sequence_number = 1 AND cm.cac_total > 0 
            THEN COALESCE(r.total_revenue, 0) / cm.cac_total
            ELSE NULL
        END as ltv_to_cac_ratio,
        
        -- Flags
        case when r.total_principal_paid >= l.funded_amount then 1 else 0 end as is_fully_paid,
        case when l.delinquency_status = 'Current' then 1 else 0 end as is_current,
        case when l.delinquency_status in ('Past due (30-59)', 'Past due (60-89)', 'Past due (90-179)', 'Past due (180<)') then 1 else 0 end as is_delinquent,
        case when r.payment_count > 0 then 1 else 0 end as has_payments
        
    from loans_with_sequence l
    left join repayments_agg r on l.loan_id = r.loan_id
    left join customer_metrics cm on l.user_id = cm.user_id
)

select * from loan_performance
