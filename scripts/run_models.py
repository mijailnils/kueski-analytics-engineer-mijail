"""Simple dbt-style runner (staging -> marts) using pandas.
Usage: python scripts/run_models.py --input data/processed --out exports
"""
import argparse
from pathlib import Path
import pandas as pd


def load_parquet(path: Path):
    return pd.read_parquet(path)


def build_repayments_mart(processed_dir: Path, out_dir: Path):
    # Expect AE_challenge_repayments.parquet
    path = processed_dir / "AE_challenge_repayments.parquet"
    df = load_parquet(path)
    # Basic cleaning
    df['event_date'] = pd.to_datetime(df['event_date'])
    # Aggregate by loan_id
    agg = df.groupby('loan_id').agg(
        repayments_count=('amount_trans', 'count'),
        total_repaid=('amount_trans', 'sum'),
        last_repayment_date=('event_date', 'max')
    ).reset_index()
    out = out_dir / 'repayments_mart.parquet'
    agg.to_parquet(out, index=False)
    print(f"Wrote {out}")
    return out


def build_loans_mart(processed_dir: Path, out_dir: Path):
    # Use customers file as loans/customer proxy
    path = processed_dir / "AE_challenge_customer.parquet"
    df = load_parquet(path)
    # example transform: keep unique loans
    out_df = df.drop_duplicates(subset=['loan_id'])
    out = out_dir / 'loans_mart.parquet'
    out_df.to_parquet(out, index=False)
    print(f"Wrote {out}")
    return out


def main(input_dir: Path, out_dir: Path):
    input_dir = Path(input_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    build_repayments_mart(input_dir, out_dir)
    build_loans_mart(input_dir, out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    main(args.input, args.out)
