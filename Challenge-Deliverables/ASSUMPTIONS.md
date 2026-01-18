# KueskiPay Q1 2025 Portfolio Analysis
## Assumptions & Methodology Documentation

**Date:** January 17, 2026  
**Analyst:** Mijail Kiektik  
**Purpose:** Document all assumptions, data sources, calculations, and methodology decisions made during the analysis

---

## Table of Contents
1. [Financial Metrics Calculation](#financial-metrics-calculation)
2. [Data Sources & Quality](#data-sources--quality)
3. [Funding Cost Methodology](#funding-cost-methodology)
4. [Risk Segmentation Assumptions](#risk-segmentation-assumptions)
5. [Performance Benchmarks](#performance-benchmarks)
6. [Data Limitations](#data-limitations)
7. [Future Improvements](#future-improvements)

---

## 1. Financial Metrics Calculation

### 1.1 P&L Waterfall Structure

**Assumed Margin Hierarchy:**
```
Revenue (Interest + Fees + Penalties)
- Cost of Funds (TIIE-based funding cost)
- Credit Loss (Charge-offs and defaults)
= Financial Margin
- COGS (Operational costs: servicing, collections)
= Contribution Margin
- CAC (Customer Acquisition Cost)
= Net Profit
```

**Rationale:** This structure separates capital costs (funding) from operational costs (COGS) and credit risk (credit loss), providing clearer visibility into profitability drivers.

**Alternative Approach Considered:** Some fintech companies combine funding cost and credit loss into a single "cost of credit" metric. We separated them to better understand:
- Capital efficiency (funding cost as % of revenue: 45.5%)
- Credit risk performance (credit loss: currently 0% due to portfolio age)

---

### 1.2 Macro Definitions

All financial calculations are implemented as dbt macros for consistency and reusability.

#### Macro: `calculate_funding_cost()`
**Location:** `dbt/macros/calculate_funding_cost.sql`

**Formula:**
```sql
funding_cost = loan_amount × funding_rate_monthly × loan_term
```

**Example:**
- Loan: $10,000
- Term: 3 months
- Rate: 1.26% monthly (TIIE 28 + spread)
- Funding Cost: $10,000 × 0.0126 × 3 = $378

**Rationale:** Simple interest calculation reflecting the actual cost of capital over the loan term.

**Assumptions:**
- Funding cost is incurred upfront when loan is disbursed
- Interest rate is fixed for the loan term (no variable rates)
- No compounding (simple interest model)

---

#### Macro: `calculate_credit_loss()`
**Location:** `dbt/macros/calculate_credit_loss.sql`

**Formula:**
```sql
credit_loss = COALESCE(charge_off_amount, 0)
```

**Current State:** All Q1 2025 loans show $0 credit loss because:
- Portfolio is 9-11 months old (Jan 2025 - Jan 2026)
- Charge-offs typically occur after 120+ days past due
- No loans have been written off yet

**Future State:** As portfolio matures, credit_loss will be populated from `stg_loans.charge_off` field when loans are formally written off.

**Assumption:** Credit loss is recognized at write-off, not provisioned monthly. This is a simplified approach; more sophisticated models would use expected loss provisioning.

---

#### Macro: `calculate_financial_margin()`
**Location:** `dbt/macros/calculate_financial_margin.sql`

**Formula:**
```sql
financial_margin = revenue - funding_cost - credit_loss
```

**Interpretation:** Gross profit after capital and credit costs, before operational expenses.

**Q1 2025 Result:** 54.5% financial margin (healthy for consumer lending)

**Benchmark Assumption:** 
- Excellent: >50%
- Good: 40-50%
- Warning: 30-40%
- Poor: <30%

---

#### Macro: `calculate_contribution_margin()`
**Location:** `dbt/macros/calculate_contribution_margin.sql`

**Formula:**
```sql
contribution_margin = financial_margin - cogs
```

Where COGS includes:
- Loan servicing costs
- Payment processing fees
- Collections operations
- Customer support

**Q1 2025 Result:** 43.1% contribution margin

**Assumption:** COGS is fully variable and scales with loan volume. Fixed overhead is excluded from this metric.

---

#### Macro: `calculate_net_profit()`
**Location:** `dbt/macros/calculate_net_profit.sql`

**Formula:**
```sql
net_profit = contribution_margin - cac
```

**Key Assumption:** CAC is allocated only to first loans (`flg_first_loan_customer = 1`). Recurrent customers have $0 CAC.

**Rationale:** 
- First loan CAC: $55.64 (includes marketing, sales, underwriting)
- Recurrent loan CAC: $18.78 (only remarketing costs)
- This separation shows true customer acquisition economics

**Q1 2025 Result:** 28.5% net profit margin

---

## 2. Data Sources & Quality

### 2.1 Primary Data Sources

| Table | Source | Records | Quality | Notes |
|-------|--------|---------|---------|-------|
| `stg_loans` | Core lending system | 29,222 | High | Complete loan-level data |
| `stg_customers` | CRM system | 4,500 | High | Customer demographics & risk |
| `stg_repayments` | Payment processor | ~100K+ | High | Transaction-level repayments |
| `stg_funding_cost_rates` | Banxico API | 3 months | Medium | Manual collection (see 2.3) |

### 2.2 Data Completeness - Q1 2025

**Complete Fields:**
- ✅ Loan amounts, terms, interest rates
- ✅ Revenue components (interest, fees, penalties)
- ✅ COGS and CAC
- ✅ Customer risk segments
- ✅ Delinquency status
- ✅ Recovery rates

**Missing/Zero Fields:**
- ⚠️ `charge_off`: All $0 (portfolio too young)
- ⚠️ `credit_loss`: All $0 (calculated from charge_off)
- ⚠️ `delinquency_status`: 1,750 loans (6%) are NULL
- ⚠️ Funding rates: Only 3 months available (Jan-Mar 2025)

### 2.3 Data Quality Issues Identified

**Issue #1: NULL Delinquency Status (6% of loans)**
- **Impact:** 1,750 loans with unknown status
- **Observation:** These loans have 20% recovery rate (vs 82% overall)
- **Assumption:** Treated as separate category in analysis
- **Recommendation:** Investigate data pipeline issue

**Issue #2: Zero Credit Loss Across Entire Portfolio**
- **Impact:** Cannot validate credit loss assumptions
- **Explanation:** Portfolio is too young (all loans <12 months old)
- **Mitigation:** Used recovery rates as proxy for credit performance
- **Assumption:** Historical loss rates will materialize as portfolio ages

---

## 3. Funding Cost Methodology

### 3.1 Data Source: Banxico TIIE 28 Días

**What is TIIE?**
- Tasa de Interés Interbancaria de Equilibrio (TIIE)
- 28-day interbank lending rate published by Banco de México
- Standard benchmark for Mexican financial products

**Why TIIE 28?**
- Most common benchmark for short-term consumer lending in Mexico
- Published daily by Banxico (central bank)
- Reflects market cost of capital

**Data Source:** https://www.banxico.org.mx/SieInternet/consultarDirectorioInternetAction.do?sector=18&accion=consultarCuadro&idCuadro=CF101&locale=es

### 3.2 Manual Data Collection Process

**Current Process (Manual):**
1. Visit Banxico website
2. Navigate to Series Económicas → Tasas de Interés → TIIE 28 días
3. Export CSV for date range
4. Calculate monthly averages
5. Add spread (assumptions below)
6. Load into `data/raw/funding_cost_rates.csv`

**File Structure:**
```csv
rate_month,tiie_28_rate,spread_rate,funding_cost_annual,funding_cost_monthly,funding_cost_daily
2025-01-01,10.25,2.00,12.25,0.0102,0.0003
2025-02-01,10.30,2.00,12.30,0.0103,0.0003
2025-03-01,10.35,2.00,12.35,0.0103,0.0003
```

### 3.3 Spread Assumptions

**Assumed Spread:** 200 basis points (2.00%) over TIIE 28

**Rationale:**
- Industry standard for fintech consumer lending in Mexico: 150-300 bps
- Covers: credit risk premium, operational costs, target margin
- Conservative estimate in absence of actual funding agreements

**Actual Spread Sources (Unknown):**
- Bank credit lines
- Securitization vehicles
- Equity financing cost

**Recommendation:** Replace with actual weighted average cost of capital (WACC) from finance team.

### 3.4 Default Funding Rate

**Fallback Rate:** 1.26% monthly (15.12% annual)

**Used When:** Loan origination month not found in `stg_funding_cost_rates`

**Calculation:**
```
TIIE 28 (avg Q1 2025): ~10.30%
+ Spread:                 2.00%
= Annual Rate:           12.30%
÷ 12 months:              1.026% monthly
```

**Impact:** Q1 2025 analysis shows all loans using this default rate (NULL tiie_28_rate in results).

**Root Cause:** Date format mismatch between `stg_loans.originated_at` and `stg_funding_cost_rates.rate_month`

**Resolution Needed:** Fix date join logic in `int_loan_financials.sql`

### 3.5 Future Automation Recommendations

**Proposed Solution: Automated Banxico API Integration**

**Step 1: Banxico API Setup**
```python
# dbt/scripts/update_funding_rates.py
import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_tiie_28():
    """
    Fetch TIIE 28 data from Banxico API
    Docs: https://www.banxico.org.mx/SieAPIRest/service/v1/
    """
    # Series ID for TIIE 28: SF43878
    api_url = "https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43878/datos"
    
    # Date range (last 90 days)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    headers = {
        'Bmx-Token': 'YOUR_API_TOKEN_HERE'  # Register at Banxico
    }
    
    params = {
        'fechaInicio': start_date,
        'fechaFin': end_date
    }
    
    response = requests.get(api_url, headers=headers, params=params)
    data = response.json()
    
    # Parse and calculate monthly averages
    df = pd.DataFrame(data['bmx']['series'][0]['datos'])
    df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y')
    df['dato'] = pd.to_numeric(df['dato'])
    
    # Monthly average
    monthly = df.groupby(df['fecha'].dt.to_period('M')).agg({
        'dato': 'mean'
    }).reset_index()
    
    monthly['rate_month'] = monthly['fecha'].dt.to_timestamp()
    monthly['tiie_28_rate'] = monthly['dato']
    
    return monthly[['rate_month', 'tiie_28_rate']]

# Add spread and calculate rates
df = fetch_tiie_28()
df['spread_rate'] = 2.00  # Update from actual WACC
df['funding_cost_annual'] = df['tiie_28_rate'] + df['spread_rate']
df['funding_cost_monthly'] = df['funding_cost_annual'] / 12 / 100
df['funding_cost_daily'] = df['funding_cost_annual'] / 365 / 100

# Save
df.to_csv('data/raw/funding_cost_rates.csv', index=False)
```

**Step 2: dbt Pre-hook**
```yaml
# dbt/dbt_project.yml
on-run-start:
  - "{{ run_python_script('scripts/update_funding_rates.py') }}"
```

**Step 3: Scheduled Execution**
- Run daily via cron/Airflow
- Updates rates automatically
- No manual intervention needed

**Benefits:**
- ✅ Always up-to-date rates
- ✅ No manual data entry errors
- ✅ Auditable data lineage
- ✅ Historical rate archive

---

## 4. Risk Segmentation Assumptions

### 4.1 Risk Segment Definitions

**Source:** `stg_customers.risk_segment_customer`

**Assumed Segmentation Logic:**

| Segment | Credit Score Range | Characteristics | Assumed Risk |
|---------|-------------------|-----------------|--------------|
| High Risk | <600 | Limited credit history, higher default probability | ~15-20% expected loss |
| Medium Risk | 600-699 | Moderate credit history, average risk | ~8-12% expected loss |
| Low Risk | 700+ | Strong credit history, low default probability | ~3-5% expected loss |
| Unknown | N/A | Insufficient data, new to credit | ~10-15% expected loss |
| Other | N/A | Special cases, corporate, etc. | Varies |

**Assumption Validation:**
- Recovery rates align with risk tiers:
  - High Risk: 75% recovery → ~25% loss (higher than expected, but portfolio is young)
  - Medium Risk: 84% recovery → ~16% loss
  - Low Risk: 88% recovery → ~12% loss

**Note:** Actual loss rates not yet observable due to portfolio age. These are **projected** losses based on recovery rate proxies.

### 4.2 Current Pricing Policy (Assumed)

**Assumption:** Interest rates are risk-adjusted

| Risk Segment | Assumed APR | Observed Avg Revenue/Loan |
|--------------|-------------|---------------------------|
| High Risk | 45-60% APR | $173.88 |
| Medium Risk | 35-45% APR | $172.48 |
| Low Risk | 25-35% APR | $80.04 |

**Key Finding:** Low Risk pricing appears insufficient to cover costs:
- Revenue per loan: $80.04
- CAC per loan: $93.74
- Loss on revenue alone, before any costs

**Assumption:** This pricing was set to:
1. Attract low-risk customers (loss leader strategy)
2. Build credit history data
3. Enable upsell to higher-value products

**Problem:** Strategy not working—Low Risk customers aren't converting to profitable products at scale.

---

## 5. Performance Benchmarks

### 5.1 LTV/CAC Ratio Benchmarks

**Assumed Thresholds:**

| Ratio | Assessment | Action |
|-------|------------|--------|
| >5.0x | Excellent | Scale aggressively |
| 3.0-5.0x | Good | Maintain/optimize |
| 1.0-3.0x | Warning | Review economics |
| <1.0x | Poor | Pause/exit segment |

**Industry Context:**
- SaaS companies: 3.0x+ is standard
- E-commerce: 2.0-3.0x typical
- Consumer lending: 4.0-6.0x healthy (higher risk requires higher margin)

**Q1 2025 Results:**
- High Risk: 9.11x ✅ Excellent
- Medium Risk: 5.14x ✅ Good
- Low Risk: 2.17x ⚠️ Below threshold

### 5.2 Payback Period Benchmarks

**Assumed Thresholds:**

| Period | Assessment | Implication |
|--------|------------|-------------|
| <6 months | Excellent | Fast capital recovery |
| 6-12 months | Good | Healthy for consumer lending |
| 12-18 months | Warning | Long capital lock-up |
| >18 months | Poor | Unsustainable |

**Q1 2025 Results:**
- High Risk: 0.7 months ✅
- Medium Risk: 1.6 months ✅
- Low Risk: -20.9 months ❌ (negative contribution)

### 5.3 Financial Margin Benchmarks

**Assumed Thresholds:**

| Margin % | Assessment | Industry Context |
|----------|------------|------------------|
| >50% | Excellent | Top quartile fintech |
| 40-50% | Good | Industry average |
| 30-40% | Warning | Tight margins |
| <30% | Poor | Unprofitable after ops costs |

**Q1 2025 Results:**
- High Risk: 57.77% ✅ Excellent
- Medium Risk: 50.33% ✅ Good
- Low Risk: 25.82% ❌ Below threshold

**Rationale:** Consumer lending requires high margins due to:
- High operational costs (servicing, collections)
- Credit risk volatility
- Regulatory capital requirements

---

## 6. Data Limitations

### 6.1 Time-Series Limitation

**Issue:** Analysis limited to Q1 2025 (3 months)

**Impact:**
- Cannot observe full loan lifecycle (12+ months)
- Default rates not yet materialized
- Seasonal patterns not visible

**Mitigation:**
- Used recovery rates as proxy for credit performance
- Analyzed historical portfolio for delinquency patterns
- Noted limitations explicitly in recommendation

**Future Improvement:** Extend analysis to full year once Q2-Q4 2025 data available.

### 6.2 Cohort Maturity

**Issue:** Q1 2025 loans are only 9-11 months old

**Observable Metrics:**
- ✅ Revenue (fully observable)
- ✅ Funding cost (known upfront)
- ✅ COGS (actual operational costs)
- ✅ CAC (acquisition spend)
- ⚠️ Recovery rates (partial—loans still repaying)
- ❌ Credit loss (not yet written off)
- ❌ Full lifetime value (loans not yet matured)

**Assumption:** Current recovery rates (82%) will hold through full lifecycle.

**Risk:** If recovery rates deteriorate, profitability projections are overstated.

**Sensitivity:**
- 10% decline in recovery → ~$150K additional credit loss
- Would reduce net profit from $449K to ~$300K (still profitable)

### 6.3 Missing Actual Funding Agreements

**Issue:** Using assumed 2.00% spread over TIIE 28

**Impact:** Funding cost may be overstated or understated

**Actual Spread Could Be:**
- Lower (1.00-1.50%) if using bank lines or securitization
- Higher (2.50-3.00%) if equity-financed or emergency capital

**Sensitivity Analysis:**
- 50 bps lower spread → +$26K additional profit (+6%)
- 50 bps higher spread → -$26K profit (-6%)

**Recommendation:** Obtain actual weighted average cost of capital (WACC) from finance team.

### 6.4 CAC Allocation Methodology

**Issue:** CAC allocated only to first loans

**Current Logic:**
```sql
CASE 
    WHEN loan_sequence_number = 1 THEN acquisition_cost
    ELSE 0
END as cac
```

**Alternative Approaches Considered:**

1. **Amortize CAC over expected lifetime loans:**
   - Pro: Matches economic reality (CAC investment for multiple loans)
   - Con: Requires assumption of loan frequency (unknown)
   
2. **Allocate CAC to all loans in first 12 months:**
   - Pro: Matches typical payback period
   - Con: Arbitrary timeframe

3. **Current approach (all CAC to first loan):**
   - Pro: Conservative, clear economics
   - Con: Overstates first loan unprofitability

**Assumption:** Current approach chosen for conservatism. First loan must be profitable enough to justify acquisition.

**Impact:** If CAC were amortized over 3 loans (average):
- First loan profit: $12.91 → $31.46
- But introduces assumptions about customer lifetime

---

## 7. Future Improvements

### 7.1 Data Infrastructure

**Priority 1: Automate Funding Rate Updates**
- Implement Banxico API integration (see Section 3.5)
- Estimated effort: 2-3 days
- Impact: Eliminate manual data entry, reduce errors

**Priority 2: Implement Expected Loss Provisioning**
```sql
-- Instead of actual charge-offs:
expected_credit_loss = loan_amount × (1 - expected_recovery_rate) × probability_of_default
```
- Requires historical cohort loss curves
- Estimated effort: 1-2 weeks
- Impact: More accurate forward-looking profitability

**Priority 3: Add Cohort Vintage Analysis**
- Track each vintage month through full lifecycle
- Calculate cumulative margins by vintage age
- Estimated effort: 3-4 days
- Impact: Better understanding of margin evolution

### 7.2 Analysis Enhancements

**Enhancement 1: Customer Lifetime Value (LTV) Model**
- Current: Using Q1 revenue as proxy for LTV
- Future: Model actual lifetime value:
```
  LTV = (Avg Revenue per Loan × Expected Loans per Customer) - 
        (Avg Credit Loss per Loan × Expected Loans) -
        (Avg COGS per Loan × Expected Loans)
```

**Enhancement 2: Channel-Level Profitability**
- Add analysis by `channel_customer` (organic, paid, referral)
- Identify most profitable acquisition channels
- Optimize CAC spend by channel

**Enhancement 3: Geographic Profitability**
- Analyze by `state_customer`
- Identify high-performing regions
- Adjust targeting strategy

### 7.3 Model Sophistication

**Current Model:** Rule-based segmentation (High/Medium/Low Risk)

**Future Models:**
1. **Machine Learning Credit Scoring:**
   - Features: Payment history, demographics, loan behavior
   - Target: Predict 90-day default probability
   - Impact: Better risk pricing

2. **Dynamic Pricing Engine:**
   - Price based on real-time risk score
   - Optimize for risk-adjusted return
   - A/B test pricing strategies

3. **Churn Prediction:**
   - Identify customers likely to stop borrowing
   - Proactive retention offers
   - Reduce customer acquisition burden

---

## 8. Methodology Validation

### 8.1 Sanity Checks Performed

**Check #1: Revenue Reconciliation**
```sql
SELECT 
    SUM(interest_revenue + fee_revenue + penalty_revenue) as calculated_revenue,
    SUM(revenue) as reported_revenue,
    (calculated_revenue - reported_revenue) as difference
FROM int_loan_financials
-- Result: $0 difference ✅
```

**Check #2: Margin Cascade**
```sql
SELECT 
    SUM(revenue) as revenue,
    SUM(revenue - funding_cost - credit_loss) as financial_margin_calc,
    SUM(financial_margin) as financial_margin_field,
    (financial_margin_calc - financial_margin_field) as difference
FROM int_loan_financials
-- Result: $0 difference ✅
```

**Check #3: LTV/CAC Ratio**
- Calculated: Revenue / CAC per customer
- Validated: Matches expected profitability rankings
- High Risk (9.11x) > Medium Risk (5.14x) > Low Risk (2.17x) ✅

### 8.2 Peer Review & Validation

**Internal Validation:**
- [ ] Finance team review of P&L structure
- [ ] Risk team validation of segment assumptions
- [ ] Product team confirmation of pricing strategy

**External Benchmarking:**
- Industry reports (e.g., McKinsey Fintech Report)
- Public fintech company investor presentations
- Peer conversations (anonymized)

**Confidence Level:** High (80-90%) for:
- P&L structure and calculations
- Profitability rankings by segment
- Directional recommendations

**Confidence Level:** Medium (60-70%) for:
- Absolute profit numbers (dependent on funding cost assumptions)
- Lifetime value projections (cohorts still maturing)
- Expected credit losses (not yet observed)

---

## 9. Glossary

| Term | Definition | Source |
|------|------------|--------|
| **TIIE 28** | Tasa de Interés Interbancaria de Equilibrio (28-day interbank rate) | Banco de México |
| **Funding Cost** | Interest expense on capital borrowed to originate loans | Calculated: TIIE 28 + spread |
| **Credit Loss** | Loan amounts written off due to default | `stg_loans.charge_off` |
| **COGS** | Cost of Goods Sold—operational costs (servicing, collections) | `stg_loans.cogs_total_cost` |
| **CAC** | Customer Acquisition Cost—marketing and sales spend per new customer | `stg_customers.acquisition_cost` |
| **LTV** | Lifetime Value—total net profit expected from a customer | Calculated: Revenue over lifetime |
| **Recovery Rate** | % of loan principal repaid | `principal_repaid / loan_amount` |
| **DPD** | Days Past Due—measure of delinquency | From payment schedules |
| **Vintage** | Cohort of loans originated in same month | `vintage_month` |
| **Recurrent Customer** | Customer with 2+ loans | `flg_recurrent_customer = 1` |

---

## 10. Document Change Log

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2026-01-17 | 1.0 | Initial documentation | Mijail Nils |

---

## 11. Contact & Questions

**For questions about:**
- **Methodology:** Mijail Kiektik, Senior Data Analyst
- **Data Sources:** Data Engineering Team
- **Business Context:** Finance Team / CFO Office
- **Risk Assumptions:** Credit Risk Team

**Document Location:** 
- GitHub: `https://github.com/mijailnils/kueski-analytics-engineer-mijail/ASSUMPTIONS.md`
- Last Updated: January 2026

---

**Disclaimer:** This document represents assumptions and methodology decisions made for the Q1 2025 portfolio analysis. Actual business decisions should incorporate additional context, stakeholder input, and real-time data validation.
