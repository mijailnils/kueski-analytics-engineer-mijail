# kueski-analytics-engineer-mijail ✅

**Goal:** Assess profitability of the KueskiPay portfolio under current acquisition and pricing policies and deliver a reproducible analytics-engineering project (data model, tests, extracts, docs).

---

## What you'll find in this repo 🔧

- `data/raw/` — place the provided CSVs here (e.g., `AE_challenge_customer.csv`, `AE_challenge_repayments.csv`).
- `data/processed/` — parquet/cleaned tables produced by ingestion scripts.
- `models/staging/` — initial staging transforms that clean and standardize raw inputs.
- `models/marts/` — analytical marts (loans, repayments, cohort_rollups) ready for BI/ML.
- `macros/` — reusable SQL/Python macros (cohorting, accrual logic, date helpers).
- `scripts/ingest.py` — script to convert CSVs → parquet and run basic quality checks.
- `tests/` — data tests (uniqueness, not_null, referential integrity) and snapshot definitions.
- `exports/` — final CSV/Parquet extracts used for visuals and analysis.
- `docs/ASSUMPTIONS.md` — explicit assumptions and trade-offs.

---

## Quickstart (local) 💡

1. Put the two CSV files in `data/raw/`.
2. Create a venv and install requirements: `python -m venv .venv && .\.venv\Scripts\activate && pip install -r scripts/requirements.txt`.
3. Run ingestion: `python scripts/ingest.py --raw data/raw --out data/processed`.
4. Run transformations locally with dbt (recommended):
   - Copy `dbt_project/profiles.yml.example` to `~/.dbt/profiles.yml` and adjust if necessary (or set `PROCESSED_DIR` env var).
   - Install dependencies: `pip install -r scripts/requirements.txt` (includes `dbt-core` and `dbt-duckdb`).
   - Run: `PROCESSED_DIR=data/processed_sample dbt run --project-dir dbt_project` and `PROCESSED_DIR=data/processed_sample dbt test --project-dir dbt_project`.
   - The dbt models are organized into `dbt_project/models/landing/`, `dbt_project/models/serving/`, and `dbt_project/models/refined/`.
5. Run tests: `pytest tests/` (contains basic data checks and will skip if no data present).

---

## Deliverables to submit

1. The repo containing models, tests, and docs (this project).
2. Extracts in `exports/` (CSV/Parquet) consumed by visuals.

---

## Notes / Next steps 🔭
- I’ll implement staged models and tests first, then snapshots for loan status/delinquency and exports for the BI layer.
- See `docs/ASSUMPTIONS.md` for assumptions and trade-offs.

---

If you want, I can (1) load the provided CSVs into `data/raw/`, (2) run the ingestion and generate sample processed files, and (3) add a first staging model for repayments and a uniqueness test. Let me know to proceed.