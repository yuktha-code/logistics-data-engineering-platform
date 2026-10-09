# Local setup and validation

This repository runs with Python 3.11+ without Docker. Work from the repository root.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe run_pipeline.py all
.\.venv\Scripts\python.exe run_pipeline.py load
.\.venv\Scripts\python.exe -m pytest -q
```

The second load should report zero newly inserted orders and events. Generated CSVs and SQLite files are local and should not be committed.

## Optional PostgreSQL

```powershell
docker compose up -d postgres
$env:WAREHOUSE_URL = 'postgresql+psycopg2://logistics_dev:logistics_dev_change_me@localhost:5433/logistics'
.\.venv\Scripts\python.exe run_pipeline.py all
```

## Optional Airflow

```powershell
docker compose --profile airflow up -d --build
docker compose exec airflow airflow dags trigger logistics_etl
```

PostgreSQL and Airflow are optional configurations; do not describe them as deployed unless they have been run and verified. Default data is synthetic. Development credentials must be changed for nonlocal use.
