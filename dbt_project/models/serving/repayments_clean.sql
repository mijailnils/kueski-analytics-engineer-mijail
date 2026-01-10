-- Serving layer: standardized, deduped repayments
with src as (
  select *
  from {{ ref('landing_repayments') }}
)
select
  user_id,
  loan_id,
  cast(event_date as timestamp) as event_date,
  cast(amount_trans as double) as amount_trans,
  cast(principalamount_trans as double) as principal,
  cast(interestamount_trans as double) as interest,
  cast(feesamount_trans as double) as fees,
  cast(penaltyamount_trans as double) as penalty,
  repayment_transaction_id
from src
where loan_id is not null
