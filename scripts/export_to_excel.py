import duckdb
import pandas as pd
from pathlib import Path

# Crear carpeta exports si no existe
Path('data/exports').mkdir(parents=True, exist_ok=True)

conn = duckdb.connect('dbt/kueski_finance.duckdb')

print("\nExportando marts a Excel...\n")

exports = {
    'fct_loan_financials': 'SELECT * FROM main.fct_loan_financials',
    'fct_cohort_performance': 'SELECT * FROM main.fct_cohort_performance',
    'fct_portfolio_pnl': 'SELECT * FROM main.fct_portfolio_pnl',
    'fct_vintage_curves': 'SELECT * FROM main.fct_vintage_curves'
}

for name, query in exports.items():
    try:
        df = conn.execute(query).df()
        filepath = f'data/exports/{name}.xlsx'
        df.to_excel(filepath, index=False, engine='openpyxl')
        print(f"OK {name}.xlsx - {len(df):,} rows x {len(df.columns)} columns")
    except Exception as e:
        print(f"ERROR {name}: {e}")

print(f"\nArchivos guardados en: data/exports/\n")

conn.close()
