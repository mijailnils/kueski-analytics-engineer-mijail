-- Refined: Loan-level metrics including cohorting and example accrual using macros
with r as (
  select * from {{ ref('repayments_clean') }}
),
first_r as (
  select
    loan_id,
    min(event_date) as first_repayment_date
  from r
  group by 1
),
agg as (
  select
    r.loan_id,
    count(*) as repayments_count,
    sum(r.amount_trans) as total_repaid,
    max(r.event_date) as last_repayment_date
  from r
  group by r.loan_id
)

select
  a.loan_id,
  a.repayments_count,
  a.total_repaid,
  a.last_repayment_date,
  f.first_repayment_date,
  -- cohort by month of first repayment (macro)
  {{ cohort_month('f.first_repayment_date') }} as cohort_month,
  -- avg repayment using safe_divide macro
  {{ safe_divide('a.total_repaid', 'a.repayments_count', 0) }} as avg_repayment,
  -- active days between first & last repayment
  datediff('day', f.first_repayment_date, a.last_repayment_date) as active_days,
  -- example accrued interest assuming an annual rate env var ASSUMED_ANNUAL_RATE (default 0.25)
  {{ accrual_amount('a.total_repaid', env_var('ASSUMED_ANNUAL_RATE', 0.25), "datediff('day', f.first_repayment_date, a.last_repayment_date)") }} as accrued_interest
from agg a
left join first_r f on a.loan_id = f.loan_id
