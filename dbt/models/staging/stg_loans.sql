{{
    config(
        materialized='view',
        tags=['staging', 'loans']
    )
}}
with source as (
    select * from read_csv_auto('C:/Users/mijai/kueski-analytics-engineer-mijail/data/raw/AE_challenge_loans.csv')
),

-- Deduplicar: quedarse con la fila del mes de originación (founded_amount > 0)
-- O si todas tienen founded_amount = 0, tomar la primera por limit_month
deduplicated as (
    select *,
        row_number() over (
            partition by loan_id 
            order by 
                case when founded_amount > 0 then 0 else 1 end,  -- Priorizar filas con funded_amount > 0
                limit_month asc  -- Si todas son 0, tomar la más antigua
        ) as row_num
    from source
),

cleaned as (
    select
        -- IDs
        loan_id,
        user_id,
        
        -- Dates
        cast(disbursed_date as timestamp) as disbursed_timestamp,
        cast(disbursed_date as date) as disbursed_date,
        strftime(cast(disbursed_date as date), '%Y-%m') as disbursed_month,
        strftime(cast(disbursed_date as date), '%Y') as disbursed_year,
        
        -- Vintage = disbursed_date (fecha de originación)
        cast(disbursed_date as date) as vintage_date,
        strftime(cast(disbursed_date as date), '%Y-%m') as vintage_month,
        strftime(cast(disbursed_date as date), '%Y') as vintage_year,
        
        -- Limit month (snapshot date)
        cast(limit_month as date) as limit_month,
        
        -- Loan attributes
        requested_amount,
        founded_amount as loan_amount,
        term as loan_term,
        interestrate as interest_rate,
        original_schedule,
        
        -- Financial metrics
        charge_off,
        cogs_total_cost,
        capital_balance,
        
        -- Funding cost (charge-offs are the actual loss)
        {{ calculate_funding_cost('charge_off') }} as funding_cost,
        
        -- Delinquency tracking
        case when delinquency_status = 'Delinquent' then 1 else 0 end as is_delinquent,
        delinquency_status,
        
        -- DPD bucket
        case 
            when delinquency_status in ('Current', 'Fully Paid') then '0_Current'
            when delinquency_status = 'Delinquent' then 'Delinquent'
            when delinquency_status is null then 'Unknown'
            else delinquency_status
        end as dpd_bucket,
        
        -- Metadata
        current_timestamp as loaded_at 
        
    from deduplicated
    where row_num = 1  -- SOLO la primera fila por loan_id
)

select * from cleaned