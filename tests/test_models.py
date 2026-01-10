import subprocess
from pathlib import Path


def test_run_models(tmp_path):
    processed = Path("data/processed")
    exports = tmp_path / "exports"
    exports.mkdir()
    # If processed dir doesn't exist, skip
    if not processed.exists():
        return
    completed = subprocess.run(["python", "scripts/run_models.py", "--input", str(processed), "--out", str(exports)], check=False)
    assert completed.returncode == 0
    # Expect repayments_mart
    assert (exports / "repayments_mart.parquet").exists()
