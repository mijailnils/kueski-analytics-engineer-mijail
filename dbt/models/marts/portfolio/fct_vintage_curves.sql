{{
    config(
        materialized='table',
        tags=['marts', 'vintage_curves']
    )
}}

-- Vintage curves: Performance por mes desde desembolso

with loan_snapshots as (
    select
        loan_id,
        user_id,
        cast(disbursed_date as date) as disbursed_date,
        strftime(cast(disbursed_date as date), '%Y-%m') as vintage_month,
        cast(limit_month as date) as snapshot_date,
        
        -- Meses desde desembolso (usando datediff)
        datediff('month', cast(disbursed_date as date), cast(limit_month as date)) as months_on_book,
        
        -- Status
        delinquency_status,
        case when delinquency_status like 'Past due%' then true else false end as is_delinquent,
        case when delinquency_status = 'Fully Paid' then true else false end as is_fully_paid,
        
        capital_balance,
        charge_off
        
    from read_csv_auto('C:/Users/mijai/kueski-analytics-engineer-mijail/data/raw/AE_challenge_loans.csv')
    where strftime(cast(disbursed_date as date), '%Y-%m') in ('2025-01', '2025-02', '2025-03')
)

select
    vintage_month,
    months_on_book,
    
    -- Contadores
    count(distinct loan_id) as loans_in_cohort,
    sum(case when is_fully_paid then 1 else 0 end) as fully_paid_count,
    sum(case when is_delinquent then 1 else 0 end) as delinquent_count,
    sum(case when delinquency_status is null then 1 else 0 end) as active_count,
    
    -- Rates (%)
    round(sum(case when is_fully_paid then 1 else 0 end) * 100.0 / count(*), 2) as fully_paid_rate,
    round(sum(case when is_delinquent then 1 else 0 end) * 100.0 / count(*), 2) as delinquency_rate,
    round(sum(case when delinquency_status is null then 1 else 0 end) * 100.0 / count(*), 2) as active_rate,
    
    -- Financials
    round(sum(capital_balance), 2) as total_capital_outstanding,
    round(sum(charge_off), 2) as total_charge_off

from loan_snapshots
group by vintage_month, months_on_book
order by vintage_month, months_on_book
