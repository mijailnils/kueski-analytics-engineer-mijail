{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

-- Staging model for customer data
-- Cleans and standardizes customer acquisition information

with source as (
    select * from read_csv_auto('C:/Users/mijai/kueski-analytics-engineer-mijail/data/raw/AE_challenge_customer.csv')
),

cleaned as (
    select
        -- IDs
        user_id,
        
        -- Dates
        cast(acquisition_date as date) as acquisition_date,
        strftime(cast(acquisition_date as date), '%Y-%m') as acquisition_month,
        
        -- Costs
        coalesce(acquisition_cost, 0) as acquisition_cost,
        
        -- Attributes
        channel,
        
        -- Risk band
        case 
            when risk_band_production = 'missing_score' then null
            when risk_band_production is null then null
            else risk_band_production
        end as risk_band_production,
        
        -- Risk segment categorization
        case
            when risk_band_production in ('1', '2') then 'Low Risk'
            when risk_band_production in ('3', '4') then 'Medium Risk'
            when risk_band_production in ('4.1', '4.2', '5') then 'High Risk'
            when risk_band_production = 'missing_score' or risk_band_production is null then 'Unknown'
            else 'Other'
        end as risk_segment,
        
        -- Geography
        city,
        state,
        current_timestamp as loaded_at
        
    from source
)

select * from cleaned