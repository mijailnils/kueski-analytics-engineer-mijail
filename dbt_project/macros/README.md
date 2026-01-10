Macros provided:

- `cohort_month(date_col)` — returns SQL that truncates `date_col` to the month start (uses date_trunc).
- `accrual_amount(principal, annual_rate, days_expr)` — computes accrued interest: (principal * annual_rate / 365) * days_expr.
- `safe_divide(numerator, denominator, default=0)` — helper to avoid division by zero.

Usage example (in a model):

  {{ cohort_month('f.first_repayment_date') }} as cohort_month
  {{ safe_divide('a.total_repaid', 'a.repayments_count', 0) }} as avg_repayment
  {{ accrual_amount('a.total_repaid', env_var('ASSUMED_ANNUAL_RATE', 0.25), "datediff('day', f.first_repayment_date, a.last_repayment_date)") }} as accrued_interest

Notes:
- These macros are intentionally simple and DB-agnostic but tested with DuckDB / dbt-duckdb in mind.
- For production, expand macros to accept more flexible date bucketing and rate inputs, and add unit tests if you have a macro-testing framework.
