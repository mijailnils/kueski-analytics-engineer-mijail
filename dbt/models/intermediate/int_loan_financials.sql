{{
    config(
        materialized='view',
        tags=['intermediate', 'financials']
    )
}}

-- Loan financials with customer attributes and loan sequence

with loans as (
    select * from {{ ref('stg_loans') }}
),

repayments as (
    select
        loan_id,
        sum(revenue_total) as total_revenue,
        sum(principal_amount) as total_principal_repaid,
        sum(interest_amount) as total_interest,
        sum(fee_amount) as total_fees,
        sum(penalty_amount) as total_penalties
    from {{ ref('stg_repayments') }}
    group by loan_id
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Calculate total loans per customer
customer_loan_counts as (
    select
        user_id,
        count(distinct loan_id) as total_loans
    from loans
    group by user_id
),

-- Calculate loan sequence per customer
loans_with_sequence as (
    select
        l.*,
        row_number() over (partition by l.user_id order by l.disbursed_date asc) as loan_sequence_number
    from loans l
),

joined as (
    select
        l.loan_id,
        l.user_id,
        l.vintage_date,
        l.vintage_month,
        l.vintage_year,
        l.loan_amount,
        l.loan_term,
        l.interest_rate,
        l.delinquency_status,
        l.is_delinquent,
        l.dpd_bucket,
        l.capital_balance,
        
        -- Loan sequence attributes
        l.loan_sequence_number,
        case when l.loan_sequence_number = 1 then 1 else 0 end as flg_first_loan_customer,
        
        -- Customer recurrency flag (calculated from loan counts)
        case when coalesce(clc.total_loans, 1) > 1 then 1 else 0 end as flg_recurrent_customer,
        
        -- Customer attributes
        c.risk_segment as risk_segment_customer,
        c.risk_band_production as risk_band_production_customer,
        c.channel as channel_customer,
        c.state as state_customer,
        c.city as city_customer,
        
        -- Revenue components
        coalesce(r.total_revenue, 0) as revenue,
        coalesce(r.total_interest, 0) as interest_revenue,
        coalesce(r.total_fees, 0) as fee_revenue,
        coalesce(r.total_penalties, 0) as penalty_revenue,
        
        -- Funding cost (charge-offs)
        l.funding_cost,
        
        -- CAC (only for first loan)
        case 
            when l.loan_sequence_number = 1 then coalesce(c.acquisition_cost, 0)
            else 0
        end as cac,
        
        -- COGS
        coalesce(l.cogs_total_cost, 0) as cogs,
        
        -- Financial margin = Revenue - Funding cost
        {{ calculate_financial_margin(
            'coalesce(r.total_revenue, 0)',
            'l.funding_cost'
        ) }} as financial_margin,
        
        -- Contribution margin = Revenue - Funding cost - COGS
        {{ calculate_contribution_margin(
            'coalesce(r.total_revenue, 0)',
            'l.funding_cost',
            'coalesce(l.cogs_total_cost, 0)'
        ) }} as contribution_margin,
        
        -- Principal repayment
        coalesce(r.total_principal_repaid, 0) as principal_repaid,
        
        -- Net profit = Contribution margin - CAC (only for first loan)
        case 
            when l.loan_sequence_number = 1 then 
                {{ calculate_contribution_margin(
                    'coalesce(r.total_revenue, 0)',
                    'l.funding_cost',
                    'coalesce(l.cogs_total_cost, 0)'
                ) }} - coalesce(c.acquisition_cost, 0)
            else 
                {{ calculate_contribution_margin(
                    'coalesce(r.total_revenue, 0)',
                    'l.funding_cost',
                    'coalesce(l.cogs_total_cost, 0)'
                ) }}
        end as net_profit,
        
        -- Recovery rate
        coalesce(r.total_principal_repaid, 0) / nullif(l.loan_amount, 0) as recovery_rate
        
    from loans_with_sequence l
    left join repayments r on l.loan_id = r.loan_id
    left join customers c on l.user_id = c.user_id
    left join customer_loan_counts clc on l.user_id = clc.user_id
)

select * from joined
