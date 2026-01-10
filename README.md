# Kueski Analytics Engineer Challenge
## Portfolio Analysis - Q1 2025 Vintages

**Goal**: Assess profitability of KueskiPay portfolio under current acquisition and pricing policies, and deliver an evidence-based recommendation to CEO/CFO.

---

## 📂 Project Structure
```
kueski-analytics-engineer-mijail/
├── README.md                          # You are here
├── EXECUTIVE_RECOMMENDATION.md        # Final recommendation (complete after analysis)
├── requirements.txt                   # Python dependencies
├── .gitignore                        # Git ignore rules
│
├── data/
│   └── raw/                          # Place CSV files here (gitignored)
│       ├── AE_challenge_customer.csv
│       ├── AE_challenge_loans.csv
│       └── AE_challenge_repayments.csv
│
├── dbt/                              # dbt project (data transformations)
│   ├── dbt_project.yml              # dbt configuration
│   ├── profiles.yml.example         # Connection template
│   ├── models/
│   │   ├── staging/                 # Layer 1: Clean raw data
│   │   ├── intermediate/            # Layer 2: Business logic
│   │   └── marts/                   # Layer 3: Final analytics tables
│   └── macros/                      # Reusable SQL functions
│
├── analysis/                         # Python analysis scripts
│   ├── 01_explore_data.py           # Explore raw CSV files
│   ├── 02_export_marts.py           # Export dbt tables to CSV
│   └── 03_qa_reconciliation.py      # Data quality checks
│
└── exports/                          # CSV files for Tableau (auto-generated)
```

---

## 🚀 Quick Start

### Step 1: Install Dependencies
```bash
# Install Python packages
pip install -r requirements.txt
```

This installs:
- `dbt-core` and `dbt-duckdb` - for data transformations
- `pandas` and `pyarrow` - for data analysis
- `duckdb` - embedded database

---

### Step 2: Prepare Data

**Small files (included in repo):**
- ✅ `data/raw/AE_challenge_customer.csv` (331 KB)
- ✅ `data/raw/AE_challenge_repayments.csv` (15 MB)

**Large file (download separately):**
- ⬇️ `data/raw/AE_challenge_loans.csv` (108 MB)

**Download the large file:**
1. Download: [AE_challenge_loans.csv (108 MB)](https://github.com/mijailnils/kueski-analytics-engineer-mijail/releases/download/v1.0-data/AE_challenge_loans.csv)
2. Place in `data/raw/` folder
3. Verify with: `ls -lh data/raw/`

**Verify data is ready:**
```bash
python analysis/01_explore_data.py
```

Expected output: Summary statistics and data quality checks.

---

### Step 3: Configure dbt
```bash
cd dbt

# Copy the example profile
cp profiles.yml.example profiles.yml

# profiles.yml is already configured for local DuckDB
# No changes needed unless you want different settings
```

---

### Step 4: Run dbt Models
```bash
# Still in dbt/ directory

# Run all transformations
dbt run --profiles-dir .

# Run data quality tests
dbt test --profiles-dir .
```

**What happens:**
1. Creates `kueski_finance.duckdb` database
2. Reads CSV files from `../data/raw/`
3. Creates staging views (clean data)
4. Creates intermediate views (business logic)
5. Creates marts tables (final analytics)

**Expected output:**
```
Completed successfully
Done. PASS=11 WARN=0 ERROR=0
```

---

### Step 5: Export Data for Analysis
```bash
# Go back to project root
cd ..

# Export marts to CSV
python analysis/02_export_marts.py
```

**Output**: CSV files created in `exports/` folder:
- `fct_loan_financials.csv` - Loan-level metrics
- `fct_cohort_performance.csv` - Cohort aggregations
- `fct_portfolio_pnl.csv` - P&L summary

---

### Step 6: Run QA Checks
```bash
python analysis/03_qa_reconciliation.py
```

**Verifies:**
- Revenue reconciliation (should match 100%)
- Loan counts across tables
- No NULL values in critical fields
- Financial metrics within expected ranges

---

## 📊 Data Model Overview

### Layer 1: Staging (`dbt/models/staging/`)

Cleans and standardizes raw data:
- `stg_customers.sql` - Customer acquisition data
- `stg_loans.sql` - Loan data with status
- `stg_repayments.sql` - Payment transactions

### Layer 2: Intermediate (`dbt/models/intermediate/`)

Applies business logic:
- `int_loans_latest.sql` - Latest loan status (handles monthly snapshots)
- `int_loans_q1_vintages.sql` - Filter to Q1 2025 only
- `int_loan_repayments_agg.sql` - Aggregate repayments per loan
- `int_loan_performance.sql` - Complete loan-level metrics

### Layer 3: Marts (`dbt/models/marts/`)

Analytics-ready tables:

**Finance:**
- `fct_loan_financials.sql` - Every loan with all financial metrics
- `fct_portfolio_pnl.sql` - P&L by vintage and risk segment

**Portfolio:**
- `fct_cohort_performance.sql` - Cohort-level KPIs

---

## 📈 Key Metrics Calculated

### Revenue Metrics
- Interest revenue (from repayments)
- Fee revenue
- Total revenue

### Cost Metrics
- Funding cost (charge-offs)
- COGS (operational costs)
- CAC (customer acquisition cost)

### Margin Metrics
- **Financial Margin** = Revenue - Funding Cost
- **Contribution Margin** = Revenue - Funding Cost - COGS
- **Net Profit** = Revenue - All Costs

### Performance Metrics
- Loss rate (charge-offs / loan volume)
- LTV/CAC ratio
- Fully paid rate
- Delinquency rate

---

## 🔍 Analysis Approach

### Risk Segmentation
Based on `risk_band_production`:
- **Low Risk**: bands 1-2
- **Medium Risk**: bands 3-4  
- **High Risk**: bands 4.1-5
- **Unknown**: missing scores

### Vintage Definition
- **Vintage Month** = Month of loan disbursement
- **Cohort** = Vintage Month + Risk Segment

### Q1 2025 Scope
Analysis focuses on loans disbursed in:
- January 2025
- February 2025
- March 2025

---

## 🧪 Testing

All models include data quality tests:
```bash
cd dbt
dbt test --profiles-dir .
```

Tests verify:
- Unique keys (no duplicates)
- Not null (required fields populated)
- Relationships (foreign keys valid)

---

## 📦 Deliverables

1. ✅ **This Repository** - Code, models, tests, documentation
2. ✅ **CSV Exports** - `exports/` folder with marts
3. ⏳ **Tableau Dashboard** - Connect to CSV exports
4. ⏳ **Executive Recommendation** - `EXECUTIVE_RECOMMENDATION.md`
5. ✅ **QA File** - `analysis/03_qa_reconciliation.py`

---

## 🔧 Troubleshooting

### "Database not found" error
```bash
cd dbt
dbt run --profiles-dir .
```

### "CSV files not found" error
Verify files are in `data/raw/` with exact names:
- `AE_challenge_customer.csv`
- `AE_challenge_loans.csv`
- `AE_challenge_repayments.csv`

### dbt tests failing
Check CSV data quality. Run exploration script:
```bash
python analysis/01_explore_data.py
```

---

## 📝 Key Assumptions

1. **Loan Snapshots**: Loans table has monthly snapshots via `limit_month`. We use the latest snapshot per loan.
2. **Revenue Attribution**: All revenue from `repayments` table is attributed to loans.
3. **Funding Cost**: Calculated from `charge_off` amounts (defaulted capital).
4. **Vintages**: Defined by loan `disbursed_date` month.

---

## 👤 Author

**Mijail** - Senior Data Analyst  
Analytics Engineer Challenge for Kueski

---

## 📄 License

This is a technical challenge project for evaluation purposes.