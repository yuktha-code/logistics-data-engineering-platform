# Porter logistics project — continuation verification (12 October 2026)

## Scope

This continuation builds upon `Porter_Logistics_MongoDB_PySpark_Upgrade.zip` and adds optional Azure Blob archiving, a reproducible local SQL benchmark, CI test plans, and fixes to advanced Airflow dependency isolation. **No remote GitHub repository was modified.** The dataset is entirely synthetic and contains no Porter customer data.

## Executed here and observed

| Check | Observed outcome | What the result supports |
| --- | --- | --- |
| Complete dependency-free pytest suite | **28 passed, 2 skipped** | Source validation, ETL replay invariants, archive SDK-style contracts, integration wiring, benchmark invariants, DAG packaging check |
| Python compilation | Passed with `compileall` | Python source is syntactically valid |
| GitHub Actions / Docker Compose YAML parsing | All files parsed | Configuration syntax only; not container runtime behavior |
| 6,000-order SQL pipeline `run_pipeline.py all` | **6,000 orders, 23,127 raw events, 23,067 validated events, all quality gates passing** | Existing SQL ETL still runs with reference workload |
| 6,000-order SQL rerun `run_pipeline.py load` | **0 new orders and 0 new events** | SQL replay safety for repeated batch |
| Local SQLite benchmark 1,000 orders, 2 runs | Median load **0.1023 s** | Local synthetic SQL baseline only |
| Local SQLite benchmark 6,000 orders, 2 runs | Median load **0.5262 s** | Local synthetic SQL baseline only |
| 25,000-order local stress run, 1 run | **96,512 raw events, 96,262 unique events, load 2.3945 s, replay 2.7697 s, 0 new rows on replay** | Local synthetic SQL larger workload, not distributed-scale performance |
| Azure archive SHA-256 / replay tests | Passed with in-memory SDK-style client double | File inventory, content-hash identity, read-back policy, failure-before-manifest behavior |
| Optional archive invocation after SQL load | Passed with fakes | Control flow sequence, not Azure deployment |
| Airflow advanced dependencies | ETL venv separation implemented and statically tested | Prevents known Airflow 2.10 / SQLAlchemy 2 environment mixing on intended Docker image; Docker build unverified |

## Not executed / cannot yet be added to the resume as achieved deployment

- Real MongoDB service bulk upsert and read query: no MongoDB process available here.
- Actual PySpark transformation and Parquet serialization: PySpark cannot be installed here (package registry network unavailable). No distributed Spark performance number has been measured.
- Actual Azure Blob SDK call or real Azure Storage account: cloud SDK unavailable here, no credentials provided. `tests/test_azure_live.py` is an opt-in integration test and was **skipped** here.
- Azurite SDK round trip: `.github/workflows/azure.yml` is written but **not executed** yet.
- Airflow container build and DAG execution: Docker not available here.
- Running Hadoop/Hive cluster, real streaming Kafka ingestion, production observability and actual cloud production workload: not part of this verification.

## Important implementation details

- Azure archive stores `city_id` / `event_month` Spark Parquet partitions with **SHA-256 content-addressed blob names**.
- Verifies remote blob properties, optionally downloads every blob to recompute its digest (enabled by default), and only publishes `_manifest.json` when all data blobs have been verified.
- A replay of byte-identical Parquet data skips existing data blobs. Rebuilt Spark Parquet files may differ byte-for-byte even with the same logical events, so **zero uploads is not guaranteed** unless bytes match.
- The cloud stage is disabled by default in CLI and Airflow. No Azure secret is hard-coded or included in reports.
- Azure archiving is not a multi-blob transaction: readers should use the last-published manifest to identify a verified archive set.
- ETL SQLAlchemy 2 requirements are installed in the **separate `/home/airflow/etl_venv`** Python venv in the advanced Airflow image, rather than overwriting Airflow 2.10.5's dependency environment.

## Portable validation steps

After downloading and extracting the complete project, follow `ADVANCED_UPGRADE_GUIDE.md` and `CLOUD_AND_BENCHMARK_GUIDE.md`:

1. Run all offline tests and original SQL replay locally.
2. Start MongoDB and run the real advanced integration test (`RUN_ADVANCED_INTEGRATION=1`).
3. Start Azurite and run the Azure SDK round-trip (`RUN_AZURE_EMULATOR_INTEGRATION=1`).
4. Run the advanced Airflow Docker image and inspect actual DAG task logs.
5. Run a real Azure account upload only with valid user-owned cloud resources, and verify download checksums.
6. Push code to a review branch in the existing repository and wait for passing GitHub Actions before strengthening resume claims.

## Resume implications

**Already defensible:** Python, SQL, SQLite, dimensional warehousing, reproducible synthetic ETL, source quality checks, idempotent loading, SQL marts and recorded local stress test.

**Implementation code exists, but real backend execution unverified here:** MongoDB, PySpark, Parquet output through Spark, Azure Blob SDK, and the advanced Airflow DAG. Mention these as designed/implemented optional integrations only if you have personally verified and can demonstrate them; do not call them deployed or production-scale.

Neither this report nor any output claims access to Porter private systems.
