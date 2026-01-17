{{
    config(
        materialized='view',
        tags=['intermediate', 'financials']
    )
}}

/*
    Loan financials with comprehensive cost structure
    
    Cost hierarchy (separated for clarity):
    1. Cost of Funds: Interest on borrowed capital (TIIE-based) → calculate_funding_cost()
    2. Credit Loss: Charge-offs and defaults → calculate_credit_loss()
    3. COGS: Operational costs (servicing, collections)
    4. CAC: Customer acquisition (first loan only)
    
    Uses macros for all margin calculations:
    - calculate_funding_cost(): Cost of capital (TIIE-based)
    - calculate_credit_loss(): Charge-offs and defaults
    - calculate_financial_margin(): Revenue - COF - Credit Loss
    - calculate_contribution_margin(): Financial Margin - COGS
    - calculate_net_profit(): Contribution Margin - CAC
    

    Margin waterfall:
    Revenue
    - Cost of Funds
    - Credit Loss
    = Financial Margin
    - COGS
    = Contribution Margin
    - CAC
    = Net Profit
*/

with loans as (
    select * from {{ ref('stg_loans') }}
),

funding_rates AS (
    SELECT * FROM {{ ref('stg_funding_cost_rates') }}
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

-- Join funding rates based on origination month
loans_with_funding as (
    select
        l.*,
        f.tiie_28_rate,
        f.spread_rate,
        f.funding_cost_annual,
        f.funding_cost_monthly,
        f.funding_cost_daily
    from loans_with_sequence l
    left join funding_rates f
        on date_trunc('month', l.loaded_at) = f.rate_month
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

        -- Funding rate components from Banxico
        l.tiie_28_rate,
        l.spread_rate,
        l.funding_cost_annual,
        l.funding_cost_monthly,
        l.funding_cost_daily,

        -- Revenue components
        coalesce(r.total_revenue, 0) as revenue,
        coalesce(r.total_interest, 0) as interest_revenue,
        coalesce(r.total_fees, 0) as fee_revenue,
        coalesce(r.total_penalties, 0) as penalty_revenue,
        

        -- COST 1: Funding cost (cost of capital)
        {{ calculate_funding_cost(
            'l.loan_amount',
            'l.loan_term',
            'coalesce(l.funding_cost_monthly, 0.0126)'
        ) }} as funding_cost,
        
        -- COST 2: Credit Loss (pérdida por defaults) - MACRO
        {{ calculate_credit_loss ('l.charge_off') }} as credit_loss,

        -- COST 3: CAC (only for first loan)
        case 
            when l.loan_sequence_number = 1 then coalesce(c.acquisition_cost, 0)
            else 0
        end as cac,
        
        -- COST 4: COGS (costos operativos)
        coalesce(l.cogs_total_cost, 0) as cogs,

        -- Principal repayment
        coalesce(r.total_principal_repaid, 0) as principal_repaid,
        
        -- Recovery metric
        coalesce(r.total_principal_repaid, 0) / nullif(l.loan_amount, 0) as recovery_rate
        
    from loans_with_funding l
    left join repayments r on l.loan_id = r.loan_id
    left join customers c on l.user_id = c.user_id
    left join customer_loan_counts clc on l.user_id = clc.user_id
    
),

final as (
    select
        *,
        
        -- MARGIN 1: Financial Margin - MACRO
        {{ calculate_financial_margin(
            'revenue',
            'funding_cost',
            'credit_loss'
        ) }} as financial_margin,
        
        -- MARGIN 2: Contribution Margin - MACRO
        {{ calculate_contribution_margin(
            'revenue',
            'funding_cost',
            'credit_loss',
            'cogs'
        ) }} as contribution_margin,
        
        -- MARGIN 3: Net Profit - MACRO
        {{ calculate_net_profit(
            'revenue',
            'funding_cost',
            'credit_loss',
            'cogs',
            'cac'
        ) }} as net_profit
        
    from joined
)

select * from final
