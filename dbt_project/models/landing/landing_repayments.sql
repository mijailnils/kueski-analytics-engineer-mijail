-- Landing: Read processed parquet into a dbt model
with src as (
  select *
  from read_parquet('{{ env_var("PROCESSED_DIR", "data/processed") }}/AE_challenge_repayments.parquet')
)
select
  user_id,
  loan_id,
  to_timestamp(event_date) as event_date,
  amount_trans,
  principalamount_trans,
  interestamount_trans,
  feesamount_trans,
  penaltyamount_trans,
  repayment_transaction_id
from src
