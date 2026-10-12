"""Requires real MongoDB, Java 17 and PySpark; enabled in CI advanced job."""
from __future__ import annotations

import os
import sys
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

pytestmark=pytest.mark.skipif(os.getenv('RUN_ADVANCED_INTEGRATION') != '1',
                              reason='requires real MongoDB and installed PySpark')


def test_real_mongo_spark_parquet_sql_roundtrip(tmp_path):
    from pymongo import MongoClient
    from pyspark.sql import SparkSession
    from src.synthetic import generate
    from run_advanced import execute
    mongo_uri=os.getenv('MONGO_URI','mongodb://localhost:27017')
    raw=tmp_path/'raw';generate(raw,35,seed=74)
    sql=f'sqlite:///{tmp_path/"warehouse.db"}'
    opts=(raw,sql,tmp_path/'results',tmp_path/'processed',tmp_path/'archive',mongo_uri)
    # Mongo collection is shared by CI tests; test event IDs are stable, not globally unique
    # across arbitrary synthetic seeds. This integration job runs against an isolated Mongo container.
    a=execute(*opts)
    n=a['spark']['deduplicated_events']
    assert a['spark']['parquet_events']==n
    assert a['mongo']['input_events']==n
    assert a['mongo']['upserted']==n
    assert a['warehouse']['warehouse_counts']['events']==n
    assert a['warehouse']['warehouse_counts']['orders']==35
    b=execute(*opts)
    assert b['mongo']['upserted']==0
    assert b['warehouse']['new_records']=={'orders':0,'events':0}
    with MongoClient(mongo_uri) as client:
        assert client.logistics_dev.tracking_events.count_documents({})==n
    spark=SparkSession.builder.master('local[2]').appName('archive-check').getOrCreate()
    try:
        assert spark.read.parquet(str(tmp_path/'archive')).count()==n
    finally:
        spark.stop()
