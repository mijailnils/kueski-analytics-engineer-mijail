{{
    config(
        materialized='view',
        tags=['staging', 'repayments']
    )
}}

-- Staging model for repayment transactions
-- One row per payment event with revenue calculations

with source as (
    select * from read_csv_auto('C:/Users/mijai/kueski-analytics-engineer-mijail/data/raw/AE_challenge_repayments.csv')
),

cleaned as (
    select
        -- IDs
        repayment_transaction_id,
        user_id,
        loan_id,
        
        -- Dates
        cast(event_date as timestamp) as event_date,
        cast(event_date as date) as event_date_only,
        strftime(cast(event_date as date), '%Y-%m') as event_month,
        
        -- Payment amounts
        amount_trans as amount_total,
        principalamount_trans as principal_amount,
        interestamount_trans as interest_amount,
        feesamount_trans as fee_amount,
        penaltyamount_trans as penalty_amount,
        
        -- Taxes
        taxoninterestamount_trans as tax_on_interest,
        taxonfeesamount_trans as tax_on_fees,
        taxonpenaltyamount_trans as tax_on_penalty,
        
        -- Revenue calculations (excluding principal which is capital return)
        interestamount_trans + feesamount_trans + penaltyamount_trans as revenue_pre_tax,
        
        -- Revenue total using macro for reusable logic
        {{ calculate_revenue(
            'interestamount_trans',
            'feesamount_trans',
            'penaltyamount_trans',
            'taxoninterestamount_trans',
            'taxonfeesamount_trans',
            'taxonpenaltyamount_trans'
        ) }} as revenue_total,

        current_timestamp as loaded_at
        
    from source
)

select * from cleaned