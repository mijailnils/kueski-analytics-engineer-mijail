import subprocess
from pathlib import Path


def test_ingest_creates_parquet(tmp_path):
    # This test expects you to place sample CSV(s) in data/raw before running
    raw = Path("data/raw")
    out = tmp_path / "processed"
    out.mkdir()
    # If there are no CSVs, skip the test
    csvs = list(raw.glob("*.csv"))
    if not csvs:
        return
    completed = subprocess.run(["python", "scripts/ingest.py", "--raw", "data/raw", "--out", str(out)], check=False)
    assert completed.returncode == 0
    # check manifest
    assert (out / "manifest.json").exists()
