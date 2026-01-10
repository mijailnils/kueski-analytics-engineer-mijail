# Assumptions & Trade-offs

- Timezones: All `event_date` fields are assumed to be in UTC unless otherwise specified.
- Schema inference: We infer types from CSVs; production systems should include explicit schemas.
- Snapshots: We'll snapshot loan status/delinquency during modeling; initial submission uses simple aggregated marts. We provide an example `loan_metrics` model that demonstrates cohorting and accrual calculations using dbt macros.
- Reproducibility: Scripts are idempotent and write deterministic files (parquet + manifest). For scale, move to dbt or a proper orchestration tool.
- Tests: Focus on uniqueness, not_null, and referential integrity for key entities (loan_id, user_id). Also note: example accrual uses an assumed annual interest rate `ASSUMED_ANNUAL_RATE` (default 25%); adjust according to product terms.

Trade-offs:
- Using pandas for transformations keeps repo simple and runnable locally; for production, dbt or Spark is recommended.

