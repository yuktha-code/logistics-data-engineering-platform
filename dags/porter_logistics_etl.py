"""Airflow 2 DAG: independent generation, source validation, atomic warehouse load."""
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
  dag_id='porter_last_mile_etl',
  description='Demo synthetic logistics: generate -> validate -> load -> quality/report',
  start_date=datetime(2026,1,1),
  schedule_interval=None,
  catchup=False,
  default_args={'owner':'portfolio_demo','retries':2,'retry_delay':timedelta(minutes=1)},
  tags=['portfolio','data-engineering','last-mile'],
) as dag:
    generate=BashOperator(task_id='generate_simulated_data',bash_command='python /opt/airflow/project/run_pipeline.py generate')
    validate=BashOperator(task_id='source_quality_gate',bash_command='python /opt/airflow/project/run_pipeline.py check')
    load=BashOperator(task_id='load_warehouse_and_marts',bash_command='python /opt/airflow/project/run_pipeline.py load')
    generate >> validate >> load
