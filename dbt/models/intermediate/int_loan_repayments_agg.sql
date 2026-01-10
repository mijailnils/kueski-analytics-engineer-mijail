{{
    config(
        materialized='view',
        tags=['intermediate', 'repayments']
    )
}}

-- Aggregate repayment data at loan level
-- Calculate total revenue components per loan

with loan_repayments as (
    select
        loan_id,
        
        -- Total payments
        sum(amount_total) as total_payments,
        sum(principal_amount) as total_principal_paid,
        
        -- Revenue components
        sum(interest_amount) as total_interest_revenue,
        sum(fee_amount) as total_fee_revenue,
        sum(penalty_amount) as total_penalty_revenue,
        
        -- Taxes
        sum(tax_on_interest) as total_tax_on_interest,
        sum(tax_on_fees) as total_tax_on_fees,
        sum(tax_on_penalty) as total_tax_on_penalty,
        
        -- Total revenue (interest + fees + penalties + taxes)
        sum(revenue_total) as total_revenue,
        sum(revenue_pre_tax) as total_revenue_pre_tax,
        
        -- Payment behavior
        count(*) as payment_count,
        min(event_date_only) as first_payment_date,
        max(event_date_only) as last_payment_date
        
    from {{ ref('stg_repayments') }}
    group by loan_id
)

select * from loan_repayments