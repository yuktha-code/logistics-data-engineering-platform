"""Optional Airflow DAG; requires the separate advanced Airflow profile.

Runs against simulated source data. Never schedule against external services
without proper authentication, resources, retention controls and monitoring.
"""
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

STEP='cd /opt/airflow/project && /home/airflow/etl_venv/bin/python advanced_stage.py'
with DAG(
    dag_id='last_mile_spark_mongodb_pipeline',
    description='Synthetic events -> quality -> Spark -> Mongo -> SQL -> optional Azure archive',
    start_date=datetime(2026,1,1),
    schedule_interval=None,
    catchup=False,
    default_args={'owner':'portfolio_demo','retries':1,'retry_delay':timedelta(minutes=1)},
    tags=['portfolio','mongodb','pyspark','logistics'],
) as dag:
    source=BashOperator(task_id='generate_synthetic_events',bash_command=f'{STEP} generate')
    validate=BashOperator(task_id='source_quality_checks',bash_command=f'{STEP} validate')
    spark=BashOperator(task_id='spark_transform_and_parquet',bash_command=f'{STEP} spark')
    mongo=BashOperator(task_id='mongo_event_mirror',bash_command=f'{STEP} mongo')
    warehouse=BashOperator(task_id='load_relational_warehouse',bash_command=f'{STEP} warehouse')
    archive=BashOperator(task_id='optional_azure_parquet_archive',bash_command=f'{STEP} archive')
    source >> validate >> spark >> mongo >> warehouse >> archive
