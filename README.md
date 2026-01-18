# Kueski Analytics Engineer Challenge
## KueskiPay Q1 2025 Portfolio Analysis

> **Objective:** Assess the profitability of the KueskiPay portfolio under current acquisition and pricing policies, and deliver an evidence-based recommendation to CEO/CFO.

**Author:** Mijail Kiektik | **Date:** January 2026

---

## 🎯 Executive Summary

**Recommendation:** Modify both acquisition and pricing policies to improve returns.

While the portfolio is profitable ($449K net profit, 28.5% margin), specific segments are losing money:

| Finding | Impact | Action |
|---------|--------|--------|
| Low Risk segment unprofitable | LTV/CAC 2.17x (below 3x threshold) | Reduce acquisition or reprice |
| Medium Risk first loans lose money | -$125.96 per loan | Fix pricing for new customers |
| High Risk recurrent = best performers | 9.11x LTV/CAC, 85% of profits | Scale up |

**Expected Impact:** +$315K/quarter (+70% profit improvement)

📄 Full analysis: [EXECUTIVE_RECOMMENDATION.md](./Challenge-Deliverables/EXECUTIVE_RECOMMENDATION.md)  
📋 Methodology: [ASSUMPTIONS.md](./Challenge-Deliverables/ASSUMPTIONS.md)

---

## 📂 Project Structure

```
kueski-analytics-engineer-mijail/
│
├── Challenge-Deliverables/
│   ├── EXECUTIVE_RECOMMENDATION.md      # Executive summary & recommendations
│   ├── ASSUMPTIONS.md                   # Methodology, calculations & assumptions
│   └── Kueski Financial P&L & Portfolio Performance - Mijail Kiektik.pdf
│
├── dbt/                                 # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── kueski_finance.duckdb            # DuckDB database
│   │
│   ├── models/
│   │   ├── staging/                     # Layer 1: Clean raw data
│   │   │   ├── schema.yml               # Tests & documentation
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_loans.sql
│   │   │   ├── stg_repayments.sql
│   │   │   └── stg_funding_cost_rates.sql
│   │   │
│   │   ├── intermediate/                # Layer 2: Business logic
│   │   │   ├── int_loans_latest.sql
│   │   │   ├── int_loans_q1_vintages.sql
│   │   │   ├── int_loan_repayments_agg.sql
│   │   │   ├── int_loan_financials.sql
│   │   │   ├── int_loan_performance.sql
│   │   │   └── int_customer_loan_metrics.sql
│   │   │
│   │   └── marts/                       # Layer 3: Analytics tables
│   │       ├── finance/
│   │       │   ├── fct_loan_financials.sql
│   │       │   ├── fct_portfolio_pnl.sql
│   │       │   └── fct_agg_performance.sql
│   │       ├── portfolio/
│   │       │   ├── fct_cohort_performance.sql
│   │       │   └── fct_vintage_curves.sql
│   │       ├── export/
│   │       │   └── customers_export.sql
│   │       ├── mart_financial_performance.sql
│   │       ├── mart_roll_rates.sql
│   │       └── mart_vintage_curves.sql
│   │
│   └── macros/                          # Reusable SQL functions
│       ├── calculate_revenue.sql
│       ├── calculate_funding_cost.sql
│       ├── calculate_credit_loss.sql
│       ├── calculate_financial_margin.sql
│       ├── calculate_cogs.sql
│       ├── calculate_contribution_margin.sql
│       ├── calculate_net_profit.sql
│       ├── dpd_bucket.sql
│       └── assign_cohort.sql
│
├── analisis_adhoc/                      # Jupyter notebooks
│   ├── 00_core_validations.ipynb        # Data quality & reconciliation
│   ├── 01_explore_customers.ipynb       # Customer dataset exploration
│   ├── 02_explore_loans.ipynb           # Loans dataset exploration
│   ├── 03_explore_repayments.ipynb      # Repayments analysis
│   ├── 04_explore_vintage_curves.ipynb  # Cohort/vintage analysis
│   ├── 05_validate_transformations.ipynb # dbt validation
│   └── core_analysis.ipynb              # Main analysis notebook
│
├── data/
│   ├── raw/                             # Source CSV files (gitignored)
│   │   ├── AE_challenge_customer.csv
│   │   ├── AE_challenge_loans.csv
│   │   └── AE_challenge_repayments.csv
│   └── exports/                         # Processed data for Tableau
│
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- dbt-core & dbt-duckdb

### 1. Install Dependencies
```bash
pip install dbt-core dbt-duckdb pandas numpy jupyter
```

### 2. Prepare Data
Place CSV files in `data/raw/`:
```
AE_challenge_customer.csv   (331 KB)
AE_challenge_loans.csv      (108 MB)
AE_challenge_repayments.csv (15 MB)
```

### 3. Run dbt
```bash
cd dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 4. Explore Analysis
```bash
jupyter notebook analisis_adhoc/
```

---

## 📊 Key Results

### P&L Waterfall (Q1 2025)

| Metric | Amount | % Revenue |
|--------|-------:|----------:|
| **Revenue** | $1,576,262 | 100.0% |
| Funding Cost | ($716,623) | 45.5% |
| **Financial Margin** | $859,639 | 54.5% |
| COGS | ($181,043) | 11.5% |
| **Contribution Margin** | $678,596 | 43.1% |
| CAC | ($229,587) | 14.6% |
| **Net Profit** | $449,009 | 28.5% |

### Performance by Risk Segment

| Segment | Loans | Profit | $/Loan | LTV/CAC | Status |
|---------|------:|-------:|-------:|--------:|--------|
| High Risk | 5,991 | $383,602 | $64.03 | 9.11x | ✅ Scale |
| Medium Risk | 2,785 | $86,854 | $31.19 | 5.14x | ⚠️ Fix pricing |
| Low Risk | 567 | -$23,943 | -$42.23 | 2.17x | 🔴 Reduce |

### Customer Type

| Type | Loans | Profit/Loan | LTV/CAC |
|------|------:|------------:|--------:|
| First Loan | 1,441 | $12.91 | 3.38x |
| Recurrent | 7,955 | $54.10 | 8.74x |

---

## 🔧 Data Model

### Staging Layer
Cleans raw data with consistent naming:
- `stg_customers` — Customer demographics & risk bands
- `stg_loans` — Loan details with monthly snapshots
- `stg_repayments` — Payment transactions
- `stg_funding_cost_rates` — TIIE 28 + spread rates

### Intermediate Layer
Applies business logic:
- `int_loans_latest` — Deduplicates to latest snapshot per loan
- `int_loans_q1_vintages` — Filters to Jan-Mar 2025
- `int_loan_repayments_agg` — Aggregates repayments per loan
- `int_loan_financials` — Calculates P&L metrics using macros
- `int_loan_performance` — Adds performance flags
- `int_customer_loan_metrics` — Customer-level aggregations

### Marts Layer
Analytics-ready tables:
- **Finance:** `fct_loan_financials`, `fct_portfolio_pnl`, `fct_agg_performance`
- **Portfolio:** `fct_cohort_performance`, `fct_vintage_curves`
- **Exports:** `customers_export`, `mart_roll_rates`

### Macros
Reusable financial calculations:
```
calculate_revenue()           → Interest + Fees + Penalties
calculate_funding_cost()      → Loan × Rate × Term
calculate_credit_loss()       → Charge-off amounts
calculate_financial_margin()  → Revenue - Funding - Credit Loss
calculate_cogs()              → Operational costs
calculate_contribution_margin() → Financial Margin - COGS
calculate_net_profit()        → Contribution Margin - CAC
dpd_bucket()                  → Days past due classification
assign_cohort()               → Vintage month assignment
```

---

## 📋 Key Assumptions

Full documentation: [ASSUMPTIONS.md](./Challenge-Deliverables/ASSUMPTIONS.md)

| Assumption | Value | Rationale |
|------------|-------|-----------|
| Funding Rate | TIIE 28 + 200 bps | Industry standard for Mexican fintech |
| Credit Loss | $0 | Portfolio <12 months, no charge-offs yet |
| CAC Allocation | 100% to first loan | Conservative; recurrent = $0 CAC |
| Risk Segments | 1-2: Low, 3-4: Medium, 4.1-5: High | Based on `risk_band_production` |

---

## ✅ Deliverables

| # | Deliverable | Status | Location |
|---|-------------|:------:|----------|
| 1 | Data model (dbt) | ✅ | `dbt/models/` |
| 2 | Transformations & tests | ✅ | `dbt/` |
| 3 | Documentation | ✅ | `Challenge-Deliverables/ASSUMPTIONS.md` |
| 4 | Marts (CSV/Excel) | ✅ | `data/exports/` |
| 5 | Tableau dashboard | ✅ | [Link in presentation] |
| 6 | Slide deck | ✅ | `Challenge-Deliverables/*.pdf` |
| 7 | Executive recommendation | ✅ | `Challenge-Deliverables/EXECUTIVE_RECOMMENDATION.md` |
| 8 | QA queries | ✅ | `analisis_adhoc/00_core_validations.ipynb` |

---

## 🧪 Testing

```bash
cd dbt
dbt test --profiles-dir .
```

**Tests include:**
- `unique` — No duplicate keys
- `not_null` — Required fields populated
- `relationships` — Foreign key integrity
- `accepted_values` — Valid data ranges

---

## 📈 Tableau

Connect to:
- CSVs in `data/exports/`
- Or directly to `dbt/kueski_finance.duckdb`

---

## 🔍 Troubleshooting

| Error | Solution |
|-------|----------|
| Database not found | `cd dbt && dbt run --profiles-dir .` |
| CSV not found | Check files in `data/raw/` |
| Tests failing | Run `jupyter notebook analisis_adhoc/00_core_validations.ipynb` |

---

## 📝 Contact

**Mijail Kiektik**  
Analytics Engineer
mijailnils@gmail.com

---

*Technical challenge for Kueski - Analytics Engineer Position*
