import pandas as pd
import sys
from pathlib import Path

# Verificar que exports exista
exports_path = Path('../exports')
if not exports_path.exists():
    print("❌ Carpeta exports no existe. Ejecuta primero:")
    print("   cd dbt")
    print("   dbt run --profiles-dir .")
    print("   cd ..")
    print("   python dbt/export_marts.py")
    sys.exit(1)

# Cargar datos
print("📂 Cargando datos...")
loans = pd.read_csv(exports_path / 'fct_loan_financials.csv')
cohorts = pd.read_csv(exports_path / 'fct_cohort_performance.csv')
pnl = pd.read_csv(exports_path / 'fct_portfolio_pnl.csv')

print(f"✅ Loans: {len(loans):,} rows")
print(f"✅ Cohorts: {len(cohorts)} rows")
print(f"✅ P&L: {len(pnl)} rows")

# Análisis básico
print("\n" + "="*80)
print("📊 RESUMEN PORTFOLIO Q1 2025")
print("="*80)

print("\n1. MÉTRICAS GENERALES:")
print(f"   Total loans: {loans['loan_id'].nunique():,}")
print(f"   Total revenue: ${loans['revenue_total'].sum():,.2f}")
print(f"   Avg revenue/loan: ${loans['revenue_total'].mean():,.2f}")
print(f"   Contribution margin: {(loans['contribution_margin'].sum() / loans['revenue_total'].sum() * 100):.2f}%")

print("\n2. POR VINTAGE:")
print(pnl.groupby('vintage_month')[['total_loans', 'total_revenue', 'ltv_to_cac_ratio']].sum())

print("\n3. POR RISK SEGMENT:")
print(pnl.groupby('risk_segment')[['total_loans', 'total_revenue', 'loss_rate']].sum())

print("\n✅ Análisis exploratorio completado")