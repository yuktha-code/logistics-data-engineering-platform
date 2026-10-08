# Exact Windows PowerShell run and GitHub upload

The ZIP contains a `Porter_LastMile_Logistics_Platform` folder. Extract it to Downloads and run:

```powershell
cd "$env:USERPROFILE\Downloads\Porter_LastMile_Logistics_Platform"
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe run_pipeline.py all
.\.venv\Scripts\python.exe run_pipeline.py load
.\.venv\Scripts\python.exe -m pytest -q
```

**Expected:** `QUALITY CHECKS PASSED: True`; `warehouse orders: 6000` and `events: 23067`; rerun produces `new_records = 0`. Save screenshots/terminal output from your own laptop for interview evidence.

## Optional local PostgreSQL

```powershell
docker compose up -d postgres
$env:WAREHOUSE_URL = 'postgresql+psycopg2://porterdev:porterdev_change_me@localhost:5433/logistics'
.\.venv\Scripts\python.exe run_pipeline.py all
```

## GitHub: create repository ONCE

```powershell
cd "$env:USERPROFILE\Downloads\Porter_LastMile_Logistics_Platform"
git init
git branch -M main
git add .
git status
git commit -m "Implement reproducible last-mile logistics ETL and quality checks"
gh repo create yuktha-code/porter-last-mile-data-engineering --public --source=. --remote=origin --push
```

If the repo already exists, do **not** run `gh repo create` again. Instead:

```powershell
git add .
git commit -m "Improve Porter logistics data platform"
git push -u origin main
```

If `nothing to commit, working tree clean`, just run `git push` (or nothing); that means no local changes remain to upload. If you already have a different `origin`, first inspect `git remote -v`; do not overwrite a remote you do not recognize.

Finally view your repo at `https://github.com/yuktha-code/porter-last-mile-data-engineering` **after** GitHub confirms creation. Do not claim that URL exists before the push succeeds.

## Airflow (advanced optional)

```powershell
docker compose --profile airflow up -d --build
docker compose exec airflow airflow dags trigger porter_last_mile_etl
```

Open `http://localhost:8080` and inspect Airflow task status. Docker/Airflow components must be tested on your own machine before presenting them as deployed experience.
