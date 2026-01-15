import duckdb
import pandas as pd

conn = duckdb.connect('dbt/kueski_finance.duckdb')

print("="*80)
print("CALCULANDO METRICAS: CAC POR PRESTAMO")
print("="*80)

# 1. Agregar metricas a nivel cliente
query_customers = """
CREATE OR REPLACE TABLE main.customers_with_metrics AS
WITH customer_loans AS (
    SELECT 
        c.user_id,
        c.acquisition_cost as cac_total,
        c.acquisition_date,
        c.channel,
        c.risk_band_production,
        c.state,
        COUNT(DISTINCT l.loan_id) as total_loans,
        ROUND(c.acquisition_cost / NULLIF(COUNT(DISTINCT l.loan_id), 0), 2) as cac_per_loan
    FROM main.stg_customers c
    LEFT JOIN main.stg_loans l ON c.user_id = l.user_id
    GROUP BY c.user_id, c.acquisition_cost, c.acquisition_date, 
             c.channel, c.risk_band_production, c.state
)
SELECT * FROM customer_loans
"""

conn.execute(query_customers)
print("OK Tabla customers_with_metrics creada")

# 2. Agregar metricas a nivel prestamo
query_loans = """
CREATE OR REPLACE TABLE main.loans_with_cac_metrics AS
WITH loan_cac AS (
    SELECT 
        l.loan_id,
        l.user_id,
        l.disbursed_date,
        l.funded_amount,
        l.interest_rate,
        c.acquisition_cost as customer_cac,
        c.total_loans as customer_total_loans,
        c.cac_per_loan,
        ROW_NUMBER() OVER (PARTITION BY l.user_id ORDER BY l.disbursed_date) as loan_number
    FROM main.stg_loans l
    LEFT JOIN main.customers_with_metrics c ON l.user_id = c.user_id
)
SELECT 
    *,
    CASE WHEN loan_number = 1 THEN customer_cac ELSE 0 END as cac_assigned
FROM loan_cac
"""

conn.execute(query_loans)
print("OK Tabla loans_with_cac_metrics creada")

# Verificar
result = conn.execute("""
SELECT 
    COUNT(*) as customers,
    ROUND(AVG(total_loans), 1) as avg_loans,
    ROUND(AVG(cac_total), 2) as avg_cac_total,
    ROUND(AVG(cac_per_loan), 2) as avg_cac_per_loan
FROM main.customers_with_metrics
WHERE total_loans > 0
""").fetchall()

print("\nRESUMEN:")
print(f"Clientes: {result[0][0]:,}")
print(f"Prestamos promedio: {result[0][1]}")
print(f"CAC total promedio: ${result[0][2]}")
print(f"CAC por prestamo: ${result[0][3]}")

conn.close()
