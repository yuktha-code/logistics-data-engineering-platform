# Merge the upgrade into the existing GitHub repository safely

Public repository: https://github.com/yuktha-code/logistics-data-engineering-platform

The current upgrade is built from your archived October 8 project snapshot, not a live GitHub checkout. GitHub's public README and name were checked, but a fresh full repository clone was unavailable in the execution environment. **Do not overwrite a newer repository with the whole archive.** Review changes and merge them, preserving any newer work.

## Safe process on your Windows laptop

1. Install Git, then clone your latest repository. Create a separate feature branch:

```powershell
git clone https://github.com/yuktha-code/logistics-data-engineering-platform.git
cd logistics-data-engineering-platform
git checkout -b feature/mongodb-pyspark
```

2. Extract the **changes-only ZIP** in another folder. Copy these new files into the corresponding repository directories:

- `run_advanced.py`, `advanced_stage.py`, `requirements-advanced.txt`, `ADVANCED_UPGRADE_GUIDE.md`, `Dockerfile.airflow-advanced`
- `src/mongo_events.py`, `src/spark_events.py`
- `tests/test_advanced.py`, `tests/test_advanced_integration.py`
- `dags/porter_logistics_advanced.py`
- `.github/workflows/advanced.yml`

3. Carefully merge (rather than blindly replace) `docker-compose.yml`, `.gitignore`, `.env.example`, and the README's new optional-upgrade section. The changes-only ZIP contains the full altered versions for comparison.

4. Run tests locally before committing:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-advanced.txt
.\.venv\Scripts\python.exe -m pytest -q
```

5. Start MongoDB and execute the real end-to-end test following `ADVANCED_UPGRADE_GUIDE.md`.

6. Check the diff, commit and push the feature branch, then open a pull request. The newly added GitHub Actions workflow will only run after the files are pushed to GitHub.

```powershell
git status
git diff --check
git add .
git commit -m "Add optional MongoDB PySpark logistics pipeline"
git push -u origin feature/mongodb-pyspark
```

7. In GitHub Actions, run or inspect **Advanced MongoDB and Spark integration**. Confirm success and inspect logs rather than assuming green status.

Never store tokens, Azure credentials or real customer data in the repository. These demonstration workloads are synthetic.


## Additional files in cloud continuation

In addition to the prior MongoDB/Spark upgrade, merge these new paths:

- `src/azure_archive.py` — replay-safe, checked Parquet Blob archive
- `archive_azure.py` — explicit cloud archive CLI
- `requirements-cloud.txt` — optional Azure Blob SDK dependencies
- `bench_sql_pipeline.py` — measured local SQL baseline, **not** Spark throughput
- `CLOUD_AND_BENCHMARK_GUIDE.md` — setup/limitations
- `tests/test_azure_archive.py`, `tests/test_azure_live.py`, `tests/test_benchmark.py`
- `.github/workflows/azure.yml` — Azure SDK/Azurite test workflow
- `benchmarks/reference_sqlite_benchmark_2026-10-12.json` — example SQLite results

Modified paths also include `run_advanced.py`, `advanced_stage.py`,
`dags/porter_logistics_advanced.py`, `Dockerfile.airflow-advanced`,
`docker-compose.yml`, `README.md`, `PORTER_ROLE_MAPPING.md`, `.env.example`,
`tests/test_advanced.py`, `.gitignore`, and this guide.

**No remote GitHub changes were made in this chat.** Push after inspecting
changes and verifying project ownership and tests.
