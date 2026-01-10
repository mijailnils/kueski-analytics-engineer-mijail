# Run dbt against the sample processed data (DuckDB)
$env:PROCESSED_DIR = "data/processed_sample"
python -m pip install -r scripts/requirements.txt
# Run dbt
dbt run --project-dir dbt_project
if(!$?) { exit 1 }
dbt test --project-dir dbt_project
if(!$?) { exit 1 }
Write-Host "dbt run + test completed successfully"
