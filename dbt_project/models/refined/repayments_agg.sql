-- Refined: Aggregated repayments per loan
with r as (
  select * from {{ ref('repayments_clean') }}
)
select
  loan_id,
  count(*) as repayments_count,
  sum(amount_trans) as total_repaid,
  max(event_date) as last_repayment_date
from r
group by loan_id
