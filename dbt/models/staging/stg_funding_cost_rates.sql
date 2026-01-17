{{
    config(
        materialized='table'
    )
}}

/*
    Staging model para tasas de funding cost
    
    Fuente: Banxico CA51 - TIIE 28 días
    Archivo: data/raw/funding_cost_rates.csv
    
    Componentes:
    - TIIE 28 días: Tasa interbancaria base
    - Spread: Costo adicional de Kueski (~6%)
    - Funding Cost: TIIE + Spread
*/

SELECT 
    CAST(rate_month AS DATE) AS rate_month,
    CAST(tiie_28_rate AS DECIMAL(10,6)) AS tiie_28_rate,
    CAST(spread_rate AS DECIMAL(10,6)) AS spread_rate,
    CAST(funding_cost_annual AS DECIMAL(10,6)) AS funding_cost_annual,
    CAST(funding_cost_monthly AS DECIMAL(10,6)) AS funding_cost_monthly,
    CAST(funding_cost_daily AS DECIMAL(10,6)) AS funding_cost_daily,
    source,
    current_timestamp AS _loaded_at
FROM read_csv_auto('../data/raw/funding_cost_rates.csv', 
    header=true
)
