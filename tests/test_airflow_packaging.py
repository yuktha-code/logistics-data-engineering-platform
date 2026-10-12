from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def test_advanced_airflow_uses_isolated_etl_environment():
    docker=(ROOT/'Dockerfile.airflow-advanced').read_text()
    dag=(ROOT/'dags/porter_logistics_advanced.py').read_text()
    assert 'python -m venv /home/airflow/etl_venv' in docker
    assert '/home/airflow/etl_venv/bin/python advanced_stage.py' in dag
    assert 'optional_azure_parquet_archive' in dag
