import pandas as pd
import json
from pathlib import Path

# Configuration
DATA_PATH = Path('../data/raw/')

print("="*80)
print("KUESKI FINANCE CHALLENGE - DATA EXPLORATION")
print("="*80)

# Check if CSV files exist
csv_files = {
    'customers': DATA_PATH / 'AE_challenge_customer.csv',
    'loans': DATA_PATH / 'AE_challenge_loans.csv',
    'repayments': DATA_PATH / 'AE_challenge_repayments.csv'
}

missing_files = [name for name, path in csv_files.items() if not path.exists()]

if missing_files:
    print(f"\n⚠️  Missing CSV files: {', '.join(missing_files)}")
    print(f"\nPlease place the CSV files in: {DATA_PATH.absolute()}")
    print("\nExpected files:")
    for name, path in csv_files.items():
        print(f"  - {path.name}")
    exit(1)

# Load datasets
print("\n1. LOADING DATASETS...")
customers = pd.read_csv(csv_files['customers'])
loans = pd.read_csv(csv_files['loans'])
repayments = pd.read_csv(csv_files['repayments'])

print(f"✓ Customers: {len(customers):,} rows")
print(f"✓ Loans: {len(loans):,} rows")
print(f"✓ Repayments: {len(repayments):,} rows")

# Customers analysis
print("\n" + "="*80)
print("2. CUSTOMERS DATASET")
print("="*80)
print("\nColumns:", list(customers.columns))
print("\nFirst 3 rows:")
print(customers.head(3))
print("\nMissing values:")
print(customers.isnull().sum())

customers['acquisition_date'] = pd.to_datetime(customers['acquisition_date'])
print("\nAcquisition date range:")
print(f"From: {customers['acquisition_date'].min()}")
print(f"To: {customers['acquisition_date'].max()}")

print("\nRisk bands distribution:")
print(customers['risk_band_production'].value_counts().sort_index())

# Loans analysis
print("\n" + "="*80)
print("3. LOANS DATASET")
print("="*80)
print("\nColumns:", list(loans.columns))
print("\nFirst 2 rows:")
print(loans.head(2))

loans['disbursed_date'] = pd.to_datetime(loans['disbursed_date'])
loans['limit_month'] = pd.to_datetime(loans['limit_month'])

print("\nDisbursed date range:")
print(f"From: {loans['disbursed_date'].min()}")
print(f"To: {loans['disbursed_date'].max()}")

print("\nDelinquency status distribution:")
print(loans['delinquency_status'].value_counts())

print(f"\nUnique loan_ids: {loans['loan_id'].nunique():,}")
print(f"Total rows: {len(loans):,}")
print(f"Rows per loan (avg): {len(loans) / loans['loan_id'].nunique():.1f}")

# Vintages
loans['vintage'] = loans['disbursed_date'].dt.to_period('M')
print("\nVintages (first 6 months):")
print(loans['vintage'].value_counts().sort_index().head(6))

# Repayments analysis
print("\n" + "="*80)
print("4. REPAYMENTS DATASET")
print("="*80)
print("\nColumns:", list(repayments.columns))

repayments['event_date'] = pd.to_datetime(repayments['event_date'])
print("\nEvent date range:")
print(f"From: {repayments['event_date'].min()}")
print(f"To: {repayments['event_date'].max()}")

print("\nRevenue components:")
revenue_cols = ['interestamount_trans', 'feesamount_trans', 'penaltyamount_trans']
for col in revenue_cols:
    print(f"  {col}: ${repayments[col].sum():,.2f}")

print(f"\nTotal repayments: ${repayments['amount_trans'].sum():,.2f}")

# Join analysis
print("\n" + "="*80)
print("5. DATA RELATIONSHIPS")
print("="*80)

print(f"\nCustomers in loans: {loans['user_id'].nunique():,}")
print(f"Total customers: {len(customers):,}")
print(f"Match rate: {loans['user_id'].nunique() / len(customers) * 100:.1f}%")

print(f"\nLoans in repayments: {repayments['loan_id'].nunique():,}")
print(f"Unique loans: {loans['loan_id'].nunique():,}")
print(f"Match rate: {repayments['loan_id'].nunique() / loans['loan_id'].nunique() * 100:.1f}%")

print("\n" + "="*80)
print("EXPLORATION COMPLETE")
print("="*80)
print("\n✓ Data looks good! Ready to run dbt models.")