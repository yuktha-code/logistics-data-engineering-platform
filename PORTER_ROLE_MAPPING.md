# Porter Data Engineering Intern — JD to implementation mapping

This is an *offline portfolio demonstration*, not an association with or access to Porter's systems.

| Responsibility in supplied JD | Demonstration in this repository | Evidence to show |
|---|---|---|
| Build / maintain data pipelines | `run_pipeline.py` + `src/pipeline.py`, with optional Airflow task orchestration | Successful CLI run, `dags/porter_logistics_etl.py` |
| Integrate sources | Separate synthetic generator and Olist order adapter | `src/synthetic.py`, `src/olist_adapter.py`, source documentation |
| SQL and NoSQL databases | SQL: relational SQLite / PostgreSQL; **optional MongoDB document upsert code** (live service unverified locally) | `sql/schema.sql`, `src/mongo_events.py`, `tests/test_advanced.py` |
| ETL ingestion / transformation | Normalize lifecycle records into staging, dimensions, facts and marts | `sql/marts.sql`, `src/pipeline.py` |
| Data accuracy, consistency and reliability | Reject schema/FK/timestamp errors; dedupe identical event IDs; audit batch counts | `src/pipeline.py`, pytest negative tests, `results/example_quality_checks.json` |
| Expose data for analytics | City/daily SLA marts, exception views, static HTML dashboard | `results/example_delivery_dashboard.html` |
| Maintainable data-processing code | Modular code, type hints, isolated seed generator, CI regression tests | `src/`, `tests/`, `.github/workflows/ci.yml` |
| Airflow, Spark, big data, cloud familiarity | Optional PySpark transformations, Parquet archival, Azure SDK upload module, and Airflow definitions; **not yet verified against live backends** | `src/spark_events.py`, `src/azure_archive.py`, `dags/porter_logistics_advanced.py`, CI workflows |

## Resume proof checklist

Do **not** claim deployment or personally completed work until run locally:

- [ ] `python run_pipeline.py all` prints `QUALITY CHECKS PASSED: True`
- [ ] `python run_pipeline.py load` returns 0 inserted orders/events
- [ ] `python -m pytest -q` passes
- [ ] `results/delivery_dashboard.html` opens and has plausible numbers
- [ ] Docker PostgreSQL integration completes, if claiming it
- [ ] Airflow DAG finishes, if claiming Airflow
- [ ] Real MongoDB/Spark integrations pass (a mock-only test is insufficient)
- [ ] Azure emulator SDK run passes before Azure SDK experience is claimed
- [ ] Real Azure storage is used if claiming actual cloud deployment
- [ ] Code is pushed to the candidate's own GitHub repository and screenshots/commands are retained

## Recommended interview explanation

“I created a small, reproducible delivery-data warehouse to practice the engineering work behind logistics analytics. The default source is simulated, with deliberate duplicate events and out-of-order ingest times. The pipeline validates keys and event time, builds a SQL warehouse with staging, fact and dimension tables, computes on-time delivery on an explicit delivered-only denominator, and produces a source-versus-target quality report. Rerunning the load does not double-count orders. I also built an optional Airflow configuration and PostgreSQL target, and I kept real driver tracking and utilization claims out of the project because I don't have that data.”
