"""Ingest raw CSVs into parquet with basic checks.
Usage: python scripts/ingest.py --raw data/raw --out data/processed
"""
import argparse
import hashlib
import json
from pathlib import Path
import pandas as pd


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_csv(path: Path, out_dir: Path) -> dict:
    # Read CSV and parse dates if present
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    # Write parquet
    out_path = out_dir / (path.stem + ".parquet")
    df.to_parquet(out_path, index=False)
    return {"file": path.name, "rows": len(df), "sha256": sha256(path), "out": str(out_path)}


def main(raw: Path, out: Path):
    raw = Path(raw)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)

    results = []
    for p in sorted(raw.glob("*.csv")):
        try:
            res = ingest_csv(p, out)
            results.append(res)
            print(f"Ingested {p.name}: {res['rows']} rows -> {res['out']}")
        except Exception as e:
            print(f"Failed to ingest {p.name}: {e}")

    # write manifest
    manifest = out / "manifest.json"
    with manifest.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"Wrote manifest: {manifest}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    main(args.raw, args.out)
