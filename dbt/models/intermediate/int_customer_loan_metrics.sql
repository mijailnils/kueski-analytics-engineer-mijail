{{
    config(
        materialized='view',
        tags=['intermediate', 'metrics']
    )
}}

-- Customer-level metrics: loans per customer and CAC per loan
-- This aggregates loan counts and calculates CAC allocation

with customers as (
    select * from {{ ref('stg_customers') }}
),

all_loans as (
    select 
        user_id,
        loan_id,
        disbursed_date
    from {{ ref('stg_loans') }}
),

customer_loan_counts as (
    select 
        c.user_id,
        c.acquisition_cost as cac_total,
        c.acquisition_date,
        c.risk_band_production,
        c.risk_segment,
        c.channel,
        c.state,
        COUNT(DISTINCT l.loan_id) as total_loans,
        ROUND(c.acquisition_cost / NULLIF(COUNT(DISTINCT l.loan_id), 0), 2) as cac_per_loan
    from customers c
    LEFT JOIN all_loans l ON c.user_id = l.user_id
    GROUP BY 
        c.user_id,
        c.acquisition_cost,
        c.acquisition_date,
        c.risk_band_production,
        c.risk_segment,
        c.channel,
        c.state
)

select * from customer_loan_counts
