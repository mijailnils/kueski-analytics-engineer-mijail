{{
    config(
        materialized='view',
        tags=['marts', 'finance']
    )
}}

-- Fact table: Loan-level financials

select * from {{ ref('int_loan_performance') }}
