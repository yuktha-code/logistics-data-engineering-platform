# Azure archival and reproducible benchmark — continuation

This guide extends the existing MongoDB/PySpark upgrade. The project uses **synthetic logistics data**, never Porter data. Archiving is **optional**, runs after PySpark has written real partitioned Parquet, and is **not enabled by default**.

## Verified in the creation environment

- Azure archive behavior was tested with an in-memory Blob SDK-style test double (checksum, content addressing, replay skipping and failure before manifest publishing).
- Actual Azure SDK upload/download could not be executed here: Azure Blob SDK could not be installed (network unavailable), and no Azure account/credentials or Azurite service was accessible.
- A separate `.github/workflows/azure.yml` runs a true Azure SDK -> Azurite emulator round trip after pushing the code. A green emulator workflow **does not establish an actual Azure-hosted deployment**.
- The existing `run_advanced.py` path remains opt-in for MongoDB and PySpark.

## Dependencies and setup on Windows

Use **Python 3.11** and **Java 17**, plus Docker Desktop for MongoDB/PostgreSQL/Azurite:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-advanced.txt
.\.venv\Scripts\python.exe -m pip install -r requirements-cloud.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe run_pipeline.py generate --orders 6000
```

### Test the Azure Blob SDK against local Azurite first

```powershell
docker compose --profile azure-emulator up -d azurite
$env:AZURE_STORAGE_CONNECTION_STRING = 'UseDevelopmentStorage=true'
$env:RUN_AZURE_EMULATOR_INTEGRATION = '1'
.\.venv\Scripts\python.exe -m pytest tests/test_azure_live.py -q -s
```

Create the local emulator container once. For the integrated pipeline, run Spark and MongoDB separately as described in `ADVANCED_UPGRADE_GUIDE.md`, then create the Blob container `logistics-archive` (the integration test creates `porter-azure-integration` but does not create the production/archive target):

```powershell
.\.venv\Scripts\python.exe -c "from azure.storage.blob import BlobServiceClient; s=BlobServiceClient.from_connection_string('UseDevelopmentStorage=true'); s.create_container('logistics-archive') if not s.get_container_client('logistics-archive').exists() else None"
.\.venv\Scripts\python.exe run_advanced.py --skip-mongo --azure-archive
.\.venv\Scripts\python.exe run_advanced.py --skip-mongo --azure-archive
```

The second run should report `uploaded=0` **if Parquet content is byte-identical**. Spark may change binary partition output across runs; in that case content addressing correctly creates new blobs rather than claiming an identical replay. The manifest always resolves the latest complete archive set. Both runs should pass checksum verification.

### Authenticate to a real Azure Blob Storage account

Azure SDK supports passwordless `DefaultAzureCredential` with an account URL (preferred), or a connection string for local dev. Create a private blob container and authorize a principal with the Storage Blob Data Contributor role. Run:

```powershell
Remove-Item Env:AZURE_STORAGE_CONNECTION_STRING -ErrorAction SilentlyContinue
$env:AZURE_STORAGE_ACCOUNT_URL = 'https://YOUR_ACCOUNT.blob.core.windows.net'
$env:AZURE_BLOB_CONTAINER = 'logistics-archive'
az login
.\.venv\Scripts\python.exe archive_azure.py
```

Or enable `--azure-archive` on `run_advanced.py` after producing Parquet. No credentials are hardcoded and the archive report contains object names, sizes, checksums and counts, not tokens. **Do not commit credentials, `.env`, generated data or cloud account identifiers.** Cloud uploads may incur charges; check storage account and budget before running.

### Airflow optional archive stage

The advanced DAG now adds `optional_azure_parquet_archive` after the SQL warehouse task. By default it prints `skipped`. To enable it, set `ENABLE_AZURE_ARCHIVE=1` and provide credentials/container variables to the Airflow advanced service. The Docker image installs cloud requirements into `/home/airflow/etl_venv`.
This venv intentionally separates ETL SQLAlchemy 2 dependencies from Airflow
2.10's own environment. The BashOperator tasks explicitly use the ETL venv.
This DAG has not been built, deployed or exercised in this runtime.

## Reproducible SQL benchmark

```powershell
.\.venv\Scripts\python.exe bench_sql_pipeline.py --sizes 1000,6000 --runs 2
```

The benchmark generates separate synthetic batches and SQLite databases, validates all source rows, loads them, and then replays the same batch. It asserts zero inserted rows on the second load. See `results/sqlite_benchmark_reference.json` for **a measured example from the creation environment**. This is **not a distributed-scale Spark benchmark**. Runtime values depend on CPU, disk and environment. Do not compare directly to production services.

## Evidence needed before resume changes

- **MongoDB:** successful actual count, query and rerun on a real MongoDB server
- **Spark:** successful actual Spark job, Parquet read-back and SQL warehouse reconciliation
- **Airflow:** green execution history in UI/logs (a DAG source file alone is not enough)
- **Azure:** successful real Azure account upload, credential-free manifest and download checksum match (Azurite is a local emulator)
- **Scalability:** larger batch runs with explicitly documented hardware and data size, excluding unsupported throughput claims

Once these pass, list those tools precisely without calling the system production-deployed or processing real logistics customers.

### Larger local SQLite stress check

A further one-run, **25,000-order** stress check is archived in
`benchmarks/reference_sqlite_25k_stress_2026-10-12.json`.
Its event total and timings are explicitly for simulated records, a local
SQLite warehouse and the available container CPU; they do not prove cloud,
MongoDB or distributed Spark performance.
