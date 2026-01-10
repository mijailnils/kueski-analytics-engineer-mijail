-- Landing: Read processed customer parquet
with src as (
  select *
  from read_parquet('{{ env_var("PROCESSED_DIR", "data/processed") }}/AE_challenge_customer.parquet')
)
select * from src
