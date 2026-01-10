import duckdb
from pathlib import Path
import os

# Configuration
DB_PATH = Path('dbt/kueski_finance.duckdb')
OUTPUT_PATH = Path('../exports/')

# Ensure output directory exists
OUTPUT_PATH.mkdir(exist_ok=True)

# Check if database exists
if not DB_PATH.exists():
    print("❌ Database not found!")
    print(f"\nExpected location: {DB_PATH.absolute()}")
    print("\nPlease run dbt first:")
    print("  cd dbt")
    print("  dbt run --profiles-dir .")
    exit(1)

# Connect to DuckDB
conn = duckdb.connect(str(DB_PATH), read_only=True)

# List of marts to export
marts = [
    'fct_loan_financials',
    'fct_cohort_performance',
    'fct_portfolio_pnl'
]

print("="*80)
print("EXPORTING MARTS TO CSV FOR TABLEAU")
print("="*80)

for mart in marts:
    print(f"\nExporting {mart}...")
    
    try:
        # Query the table
        query = f"SELECT * FROM main.{mart}"
        df = conn.execute(query).df()
        
        # Export to CSV
        output_file = OUTPUT_PATH / f"{mart}.csv"
        df.to_csv(output_file, index=False)
        
        print(f"✓ Exported {len(df):,} rows to {output_file.name}")
        print(f"  Columns: {len(df.columns)}")
        
    except Exception as e:
        print(f"❌ Error exporting {mart}: {e}")

# Generate summary statistics
print("\n" + "="*80)
print("PORTFOLIO SUMMARY")
print("="*80)

try:
    summary_query = """
    SELECT
        vintage_month,
        risk_segment,
        total_loans,
        total_customers,
        ROUND(gross_loan_volume, 2) as gross_loan_volume,
        ROUND(total_revenue, 2) as total_revenue,
        ROUND(contribution_margin, 2) as contribution_margin,
        ROUND(contribution_margin_pct * 100, 2) as contribution_margin_pct,
        ROUND(loss_rate * 100, 2) as loss_rate_pct,
        ROUND(ltv_to_cac_ratio, 2) as ltv_cac
    FROM main.fct_portfolio_pnl
    ORDER BY vintage_month, risk_segment
    """
    
    summary_df = conn.execute(summary_query).df()
    print("\n", summary_df.to_string(index=False))
    
except Exception as e:
    print(f"❌ Error generating summary: {e}")

# Overall totals
try:
    totals_query = """
    SELECT
        'TOTAL Q1 2025' as period,
        SUM(total_loans) as total_loans,
        ROUND(SUM(gross_loan_volume), 2) as gross_loan_volume,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(SUM(contribution_margin), 2) as contribution_margin,
        ROUND(SUM(contribution_margin) / NULLIF(SUM(total_revenue), 0) * 100, 2) as margin_pct,
        ROUND(SUM(funding_cost) / NULLIF(SUM(gross_loan_volume), 0) * 100, 2) as loss_rate_pct
    FROM main.fct_portfolio_pnl
    """
    
    totals_df = conn.execute(totals_query).df()
    print("\n" + "="*80)
    print("OVERALL Q1 2025 TOTALS")
    print("="*80)
    print("\n", totals_df.to_string(index=False))
    
except Exception as e:
    print(f"❌ Error generating totals: {e}")

conn.close()

print("\n" + "="*80)
print("EXPORT COMPLETE")
print("="*80)
print(f"\nAll files saved to: {OUTPUT_PATH.absolute()}")