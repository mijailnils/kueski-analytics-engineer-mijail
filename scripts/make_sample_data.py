"""Create small sample processed parquet files from full processed data.
Usage: python scripts/make_sample_data.py --processed data/processed --out data/processed_sample --n 100
"""
import argparse
from pathlib import Path
import pandas as pd


def sample_parquet(in_path: Path, out_path: Path, n: int=100):
    df = pd.read_parquet(in_path)
    sample = df.head(n) if len(df) > n else df
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sample.to_parquet(out_path, index=False)
    print(f"Wrote sample: {out_path} ({len(sample)} rows)")


def main(processed: Path, out: Path, n: int):
    processed = Path(processed)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)

    for p in processed.glob('*.parquet'):
        sample_parquet(p, out / p.name, n=n)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--processed', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--n', type=int, default=100)
    args = parser.parse_args()
    main(args.processed, args.out, args.n)
