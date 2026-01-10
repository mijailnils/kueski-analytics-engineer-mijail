import duckdb
from pathlib import Path

# Configuration
DB_PATH = Path('../dbt/kueski_finance.duckdb')

# Check if database exists
if not DB_PATH.exists():
    print("❌ Database not found!")
    print(f"\nExpected location: {DB_PATH.absolute()}")
    print("\nPlease run dbt first:")
    print("  cd dbt")
    print("  dbt run --profiles-dir .")
    exit(1)

conn = duckdb.connect(str(DB_PATH), read_only=True)

print("="*80)
print("QUALITY ASSURANCE & RECONCILIATION REPORT")
print("="*80)

# 1. Row counts
print("\n1. DATA COMPLETENESS CHECKS")
print("-"*80)

try:
    staging_counts = conn.execute("""
        SELECT 'stg_customers' as table_name, COUNT(*) as row_count FROM main.stg_customers
        UNION ALL
        SELECT 'stg_loans', COUNT(*) FROM main.stg_loans
        UNION ALL
        SELECT 'stg_repayments', COUNT(*) FROM main.stg_repayments
    """).df()
    
    print("\nStaging layer:")
    print(staging_counts.to_string(index=False))
except Exception as e:
    print(f"❌ Error checking staging: {e}")

try:
    marts_counts = conn.execute("""
        SELECT 'fct_loan_financials' as table_name, COUNT(*) as row_count FROM main.fct_loan_financials
        UNION ALL
        SELECT 'fct_cohort_performance', COUNT(*) FROM main.fct_cohort_performance
        UNION ALL
        SELECT 'fct_portfolio_pnl', COUNT(*) FROM main.fct_portfolio_pnl
    """).df()
    
    print("\nMarts layer:")
    print(marts_counts.to_string(index=False))
except Exception as e:
    print(f"❌ Error checking marts: {e}")

# 2. Revenue reconciliation
print("\n\n2. REVENUE RECONCILIATION")
print("-"*80)

try:
    revenue_recon = conn.execute("""
        WITH repayments_total AS (
            SELECT SUM(revenue_total) as repayments_revenue
            FROM main.stg_repayments
            WHERE loan_id IN (SELECT loan_id FROM main.int_loans_q1_vintages)
        ),
        marts_total AS (
            SELECT SUM(revenue_total) as marts_revenue
            FROM main.fct_loan_financials
        )
        SELECT 
            ROUND(r.repayments_revenue, 2) as repayments_revenue,
            ROUND(m.marts_revenue, 2) as marts_revenue,
            ROUND(r.repayments_revenue - m.marts_revenue, 2) as difference,
            ROUND(ABS(r.repayments_revenue - m.marts_revenue) / NULLIF(r.repayments_revenue, 0) * 100, 4) as pct_difference
        FROM repayments_total r, marts_total m
    """).df()
    
    print("\n", revenue_recon.to_string(index=False))
    
    if revenue_recon['pct_difference'].iloc[0] < 0.01:
        print("✓ Revenue reconciliation PASSED (< 0.01% difference)")
    else:
        print("⚠ Revenue reconciliation WARNING (>= 0.01% difference)")
except Exception as e:
    print(f"❌ Error in revenue reconciliation: {e}")

# 3. Loan count validation
print("\n\n3. LOAN COUNT VALIDATION")
print("-"*80)

try:
    loan_count = conn.execute("""
        SELECT
            'Q1 Vintages (int)' as source,
            COUNT(DISTINCT loan_id) as unique_loans
        FROM main.int_loans_q1_vintages
        UNION ALL
        SELECT 'Loan Financials (mart)', COUNT(DISTINCT loan_id)
        FROM main.fct_loan_financials
        UNION ALL
        SELECT 'Cohort Performance (sum)', SUM(loan_count)
        FROM main.fct_cohort_performance
    """).df()
    
    print("\n", loan_count.to_string(index=False))
    
    if loan_count['unique_loans'].nunique() == 1:
        print("✓ Loan counts MATCH across all tables")
    else:
        print("⚠ Loan counts DO NOT match")
except Exception as e:
    print(f"❌ Error in loan count validation: {e}")

# 4. NULL checks
print("\n\n4. NULL VALUE CHECKS")
print("-"*80)

try:
    null_checks = conn.execute("""
        SELECT
            SUM(CASE WHEN loan_id IS NULL THEN 1 ELSE 0 END) as null_loan_ids,
            SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) as null_user_ids,
            SUM(CASE WHEN vintage_month IS NULL THEN 1 ELSE 0 END) as null_vintage,
            SUM(CASE WHEN revenue_total IS NULL THEN 1 ELSE 0 END) as null_revenue,
            SUM(CASE WHEN contribution_margin IS NULL THEN 1 ELSE 0 END) as null_margin
        FROM main.fct_loan_financials
    """).df()
    
    print("\n", null_checks.to_string(index=False))
    
    if null_checks.sum().sum() == 0:
        print("✓ No NULL values in critical fields")
    else:
        print("⚠ NULL values detected")
except Exception as e:
    print(f"❌ Error in NULL checks: {e}")

# 5. Financial sanity checks
print("\n\n5. FINANCIAL METRICS SANITY CHECKS")
print("-"*80)

try:
    sanity = conn.execute("""
        SELECT
            COUNT(*) as total_loans,
            SUM(CASE WHEN revenue_total < 0 THEN 1 ELSE 0 END) as negative_revenue,
            SUM(CASE WHEN funding_cost < 0 THEN 1 ELSE 0 END) as negative_funding_cost,
            SUM(CASE WHEN loss_rate < 0 THEN 1 ELSE 0 END) as negative_loss_rate
        FROM main.fct_loan_financials
    """).df()
    
    print("\n", sanity.to_string(index=False))
    
    issues = sanity[['negative_revenue', 'negative_funding_cost', 'negative_loss_rate']].sum().sum()
    if issues == 0:
        print("✓ All financial metrics within expected ranges")
    else:
        print("⚠ Some financial metrics outside expected ranges")
except Exception as e:
    print(f"❌ Error in sanity checks: {e}")

conn.close()

print("\n" + "="*80)
print("QA REPORT COMPLETE")
print("="*80)