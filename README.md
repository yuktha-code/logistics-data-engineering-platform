# Logistics Data Engineering Platform — Delivery Reliability

[![ETL quality and rerun safety](https://github.com/yuktha-code/logistics-data-engineering-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/yuktha-code/logistics-data-engineering-platform/actions/workflows/ci.yml)

**A reproducible data-engineering portfolio project built around the operational data problems of last-mile logistics.**

Python 3.11 · SQLAlchemy · SQLite (zero-Docker local path) · PostgreSQL 16 (Docker path) · Apache Airflow 2.10 (optional orchestration) · SQL · pytest · GitHub Actions

> **Data provenance:** The default project is built entirely from **simulated logistics orders, synthetic driver IDs and simulated GPS/status events**. It is **not a production logistics system** and has **no access to any provider's private data**. Optionally, it can process the separate public **Olist Brazilian e-commerce order dataset** without inventing drivers, GPS coordinates or delivery revenue.

## What the project demonstrates

- Multi-source ingestion from CSV with explicit schemas, stable business keys and source labels.
- **Bronze / staging:** `stg_orders`, `stg_events`; **dimensions:** `dim_city`, `dim_customer`, `dim_driver`; **facts:** `fact_delivery`, `fact_delivery_event`.
- **Idempotent upserts** using primary-key conflict handling, so rerunning a batch does not create duplicate facts.
- Transactional warehouse writes and a `pipeline_audit` trail with ingested rows, deduplicated events and inserted counts.
- Automated **data quality gates**: schema presence, conflicting duplicate rejection, source-parent FK integrity, city/driver consistency, plausible timestamps, nonnegative fares, valid coordinates and warehouse FK/row-count checks.
- SQL views for on-time delivery, delayed deliveries, cancel/failed exceptions and city-day performance, with a **defined eligible-delivery denominator**.
- Optional Airflow DAG with `generate -> validate -> load` tasks and retries, plus a Dockerized PostgreSQL target.
- CI regression tests for invalid source data, idempotence and an Olist adapter fixture.

## Data flow

```mermaid
flowchart TD
 A[Deterministic simulated CSV or public Olist CSV] --> B[Schema and duplicate validation]
 B --> C{Quality gate passes?}
 C -->|No| X[Reject batch with error]
 C -->|Yes| D[SQL staging tables]
 D --> E[Dimensional warehouse facts and dimensions]
 E --> F[Foreign key and row-count checks]
 F --> G{Warehouse checks pass?}
 G -->|No| X
 G -->|Yes| H[SQL marts: daily, city, exceptions, driver-day]
 H --> I[JSON report and pipeline audit]
```

## 1. Run immediately on Windows (no Docker required)

From PowerShell, enter the **project folder containing `run_pipeline.py`**:

```powershell
cd "$env:USERPROFILE\Downloads\logistics-data-engineering-platform"
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe run_pipeline.py all
.\.venv\Scripts\python.exe -m pytest -q
```

If `py -3.11` is unavailable but Python 3.11+ is installed, replace `py -3.11` with `python`.

Results are written to `results/pipeline_report.json` and `results/quality_checks.json` and a browser-viewable static dashboard at `results/delivery_dashboard.html`. A local SQLite warehouse is saved at `data/warehouse.sqlite` (ignored by Git). **Do not upload virtual environments or raw generated CSVs.**

**Prove idempotency:**

```powershell
.\.venv\Scripts\python.exe run_pipeline.py load
```

The second load should say `"orders": 0, "events": 0` under NEW RECORDS. To simulate incremental ingestion (preserving the original first 6,000 stable IDs):

```powershell
.\.venv\Scripts\python.exe run_pipeline.py generate --orders 6050
.\.venv\Scripts\python.exe run_pipeline.py load
```

The warehouse should now contain 6,050 distinct orders and the next rerun should insert 0. Running `all` regenerates the 6,000-record default source; it does **not** remove already loaded records.

## 2. PostgreSQL (Docker Desktop)

With Docker Desktop running:

```powershell
docker compose up -d postgres
$env:WAREHOUSE_URL = 'postgresql+psycopg2://logistics_dev:logistics_dev_change_me@localhost:5433/logistics'
.\.venv\Scripts\python.exe run_pipeline.py all
.\.venv\Scripts\python.exe run_pipeline.py load
```

Inspect:

```powershell
docker compose exec postgres psql -U logistics_dev -d logistics -c 'SELECT * FROM mart_city_performance ORDER BY total_orders DESC;'
docker compose exec postgres psql -U logistics_dev -d logistics -c 'SELECT * FROM pipeline_audit ORDER BY run_finished DESC LIMIT 3;'
```

The Docker configuration uses **development-only default credentials**. Never reuse these credentials in a real deployment.

## 3. Optional Airflow orchestration (Docker)

Airflow is not needed to run the ETL; it is an optional layer that demonstrates dependencies, retries and monitoring. It requires additional CPU/RAM and takes longer to install.

```powershell
docker compose --profile airflow up -d --build
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags trigger logistics_etl
```

Open <http://localhost:8080> to inspect the DAG. To retrieve the automatically generated standalone admin password:

```powershell
docker compose exec airflow cat /opt/airflow/airflow/standalone_admin_password.txt
```

The DAG sets `WAREHOUSE_URL` to Docker's PostgreSQL hostname `postgres:5432`. It invokes the same tested Python code via `BashOperator` tasks; you do not need to change the ETL to use PostgreSQL.

**Verification boundary:** The standalone SQLite implementation and tests have been executed. This package also contains PostgreSQL/Docker/Airflow configurations, but those components must be run on a machine with Docker before claiming end-to-end Docker deployment.

## 4. Optional real, public Olist historical orders

Olist's **Brazilian E-Commerce Public Dataset** provides historical e-commerce orders and customer locations, not private carrier trip histories or telematics. Obtain `olist_orders_dataset.csv` and `olist_customers_dataset.csv` from the [Olist public Kaggle dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), and place both in `data/olist/`.

```powershell
.\.venv\Scripts\python.exe run_pipeline.py olist --limit 5000 --database-url 'sqlite:///data/olist_warehouse.sqlite'
```

**Important:** Use a **separate** warehouse from the synthetic one, as shown. Olist estimated delivery is date-level and is converted to end-of-day. `driver_id`, GPS coordinates, shipping distance and fare are left missing, not fabricated. `CREATED`, `PICKED_UP`, `DELIVERED` events are reconstructed from recorded milestones; they are **not GPS pings or directly observed tracking events**. Olist contains Brazilian customer city/state data, so do not market it as Indian logistics or private logistics operational data.

## 5. Query the warehouse

Run in PostgreSQL or any SQLite client:

```sql
-- Cities with lowest on-time delivery rates
SELECT city, state, total_orders, delivered_orders, on_time_pct,
       avg_delay_minutes
FROM mart_city_performance
ORDER BY on_time_pct ASC;

-- Day-level delivery SLA monitoring
SELECT order_day, city_id, orders, delivered, on_time_pct, cancelled, failed
FROM mart_delivery_daily
ORDER BY order_day DESC;

-- Exception-level diagnostic records
SELECT city, exception_type, COUNT(*) AS incidents
FROM mart_delivery_exceptions
GROUP BY city, exception_type
ORDER BY incidents DESC;

-- Freshness proxy: most recent ingested milestone event
SELECT MAX(ingested_at) AS latest_event_ingest_time FROM fact_delivery_event;
```

## 6. Measurements and semantics

**On-time % = delivered on/before promised timestamp / all delivered orders × 100.** A cancelled or failed order is not counted in that denominator; report cancellation and failure separately. `delay_minutes` is zero for early/on-time arrivals, positive for late deliveries and NULL if not delivered. The `synthetic_gross_fare_inr` metric is **simulated gross booked fare**, not collected revenue or contribution margin. `mart_driver_daily` records daily **assignments/completions**, not driver utilization because available/on-duty driver hours are not measured.

For the deterministic default simulation (`--orders 6000 --seed 42`), the first executed run produced 6,000 orders (5,411 marked delivered), 23,067 distinct event records and 60 removed duplicate raw event rows, with an overall on-time value of 50.73% on delivered orders. These figures are deliberately simulation-dependent, **not observations or claims about any logistics operator**. Example outputs are committed in `results/example_pipeline_report.json` and `results/example_quality_checks.json`.

## 7. Project structure

```text
logistics-data-engineering-platform/
  run_pipeline.py            # generate / load / check / olist commands
  src/
    synthetic.py             # deterministic simulated lifecycle and GPS events
    olist_adapter.py         # optional historical Olist mapping
    pipeline.py              # prevalidation + SQL upserts + quality gates
  sql/
    schema.sql               # staging + dimensional tables, indexes, audit
    marts.sql                # analytical warehouse views
  dags/
    logistics_etl.py  # optional Airflow orchestration
  tests/
    test_pipeline.py         # regression tests and failure cases
  results/
    example_pipeline_report.json
    example_quality_checks.json
    example_delivery_dashboard.html
  data/raw/                 # generated on execution; not committed
  data/olist/               # optional external files; not committed
  docker-compose.yml
  Dockerfile.airflow
  requirements.txt
  DATA_SOURCES.md
  README.md
  .github/workflows/ci.yml
```

## Reproducibility and limitations

1. Default events are deterministic simulations with deliberately injected identical event duplicates and out-of-order arrival. They are not real orders/transactions/driver locations.
2. System uses in-memory CSV parsing and row-wise validation before batch database upserts. This is appropriate for a reproducible internship portfolio prototype, **not a benchmark of multi-terabyte Spark ETL**.
3. Late-arriving milestones are handled by revisiting stable keys on rerun rather than filtering exclusively on event timestamp watermarks. `pipeline_audit` records inserted counts, not per-column change history.
4. Geo coordinates are simulated near city centers and **must not be used for navigation, routing or real ETA calculations**.
5. The core SQL ETL does not use Kafka or Spark. Optional PySpark, MongoDB and Azure SDK modules are included as source-code prototypes; live service integration and cloud deployment are not verified.
6. Data checks guard common source errors, but no individual customer address, driver identity or real-world SLA calibration is included.
7. Olist input may have dirty source records; quality failures are surfaced rather than silently rewritten.

## Engineering design decisions

- **Why conflict-key upserts?** Replays or retries of the same raw batch must not duplicate facts; metrics and audit counts remain interpretable.
- **Why validate before insert?** Orphan GPS/order events, impossible timestamps and duplicate IDs must be rejected before corrupting a warehouse.
- **How are denominators defined?** Delivery timeliness evaluates only fulfilled deliveries, while failures and cancellations appear separately. Mixing them changes the business meaning of the KPI.
- **What happens if a job is rerun?** Rows keep stable IDs. A second batch modifies existing records if values changed and otherwise adds no new facts.
- **Why Airflow?** To manage task dependencies, retry failures and inspect run history; core ETL remains runnable without the orchestrator.
- **What would you add in production?** CDC/incremental watermark by `ingested_at`, partitioned object storage, Great Expectations/dbt tests, observability/alerts, secret management and real performance benchmarking.

## Scope

This is an independent, reproducible engineering demonstration. All default-order, driver, fare, and GPS figures are simulated, and no proprietary operational data is used.
## Optional MongoDB, PySpark and Azure extensions (source-code prototype)

The original tested SQL-based logistics pipeline remains the default. Separate optional modules add:

- MongoDB raw-event document upserts (`src/mongo_events.py`).
- PySpark event transformations, deduplication and partitioned Parquet (`src/spark_events.py`).
- Azure Blob SDK archival, SHA-256 verification and manifests (`src/azure_archive.py`).
- Advanced Airflow orchestration and opt-in Docker services (`dags/porter_logistics_advanced.py`, `docker-compose.yml`).
- Offline regression tests and a **local SQLite** stress benchmark (`bench_sql_pipeline.py`).

Start with [`ADVANCED_UPGRADE_GUIDE.md`](ADVANCED_UPGRADE_GUIDE.md) and [`CLOUD_AND_BENCHMARK_GUIDE.md`](CLOUD_AND_BENCHMARK_GUIDE.md).

**Evidence boundary:** These advanced components are optional prototypes. The prior offline verification reported 28 tests passed, two live-service checks skipped. MongoDB/PySpark/Azure/Airflow live integrations and a real Azure cloud deployment still require separate execution before claiming them. The 25,000-order benchmark uses **synthetic orders and SQLite**, not production or distributed Spark data.