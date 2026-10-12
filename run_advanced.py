"""Optional advanced mode: validated source -> Mongo raw mirror + PySpark -> SQL marts."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from src.pipeline import DataQualityError, clean_validate, load, read_csv_tables
from src.mongo_events import mirror_events_to_mongo
from src.spark_events import transform_events_with_spark

ROOT = Path(__file__).resolve().parent


def execute(raw_dir: Path, database_url: str, results_dir: Path, processed_dir: Path,
            parquet_dir: Path, mongo_uri: str, use_mongo: bool = True,
            use_spark: bool = True, spark_master: str = 'local[2]',
            azure_archive: bool = False, azure_container: str = 'logistics-archive',
            azure_prefix: str = 'logistics/events/v1') -> dict:
    """Sources are never mutated; each stage can safely be re-run.

    There is no distributed ACID transaction across MongoDB, Spark output and SQL.
    A failure stops subsequent steps; reruns reconcile by stable event/order IDs.
    """
    tables, counts = read_csv_tables(raw_dir)
    clean_validate(tables)
    report: dict = {'source_rows':counts, 'mongo':None, 'spark':None, 'azure':None}
    if use_spark:
        report['spark'] = transform_events_with_spark(
            raw_dir, processed_dir, parquet_dir,
            expected_unique=counts['events']['distinct'],
            expected_raw=counts['events']['raw'], master=spark_master)
        effective_raw = processed_dir
    else:
        effective_raw = raw_dir
    if azure_archive and not use_spark:
        raise ValueError('Azure Parquet archive requires --skip-spark to be disabled')
    if use_mongo:
        report['mongo'] = mirror_events_to_mongo(tables['events'], mongo_uri)
    report['warehouse'] = load(effective_raw, database_url, ROOT/'sql', results_dir)
    if report['warehouse']['warehouse_counts']['events'] < counts['events']['distinct']:
        raise DataQualityError('warehouse event reconciliation mismatch')
    if azure_archive:
        from src.azure_archive import upload_to_azure
        report['azure'] = upload_to_azure(
            parquet_dir, azure_container,
            connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
            account_url=os.getenv('AZURE_STORAGE_ACCOUNT_URL'),
            prefix=azure_prefix)
    results_dir.mkdir(parents=True,exist_ok=True)
    (results_dir/'advanced_run.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
    return report


def main(argv=None) -> int:
    p=argparse.ArgumentParser(description='Advanced logistics Spark + MongoDB ETL')
    p.add_argument('--raw-dir',type=Path,default=ROOT/'data'/'raw')
    p.add_argument('--processed-dir',type=Path,default=ROOT/'data'/'processed')
    p.add_argument('--parquet-dir',type=Path,default=ROOT/'data'/'parquet'/'delivery_events')
    p.add_argument('--results-dir',type=Path,default=ROOT/'results')
    p.add_argument('--database-url',default=os.getenv('WAREHOUSE_URL',f'sqlite:///{ROOT / "data" / "warehouse.sqlite"}'))
    p.add_argument('--mongo-uri',default=os.getenv('MONGO_URI','mongodb://localhost:27018'))
    p.add_argument('--spark-master',default='local[2]')
    p.add_argument('--skip-mongo',action='store_true',help='Local Spark/SQL path without MongoDB')
    p.add_argument('--skip-spark',action='store_true',help='MongoDB mirror + existing SQL path')
    p.add_argument('--azure-archive',action='store_true',help='Sync Spark Parquet to configured Azure Blob Storage')
    p.add_argument('--azure-container',default=os.getenv('AZURE_BLOB_CONTAINER','logistics-archive'))
    p.add_argument('--azure-prefix',default=os.getenv('AZURE_BLOB_PREFIX','logistics/events/v1'))
    args=p.parse_args(argv)
    if args.skip_mongo and args.skip_spark:
        p.error('Use run_pipeline.py load when both optional integrations are disabled')
    try:
        report=execute(args.raw_dir,args.database_url,args.results_dir,args.processed_dir,
                       args.parquet_dir,args.mongo_uri,not args.skip_mongo,
                       not args.skip_spark,args.spark_master,
                       args.azure_archive,args.azure_container,args.azure_prefix)
        print(json.dumps({'mongo':report['mongo'],'spark':report['spark'],
                          'azure':None if report['azure'] is None else {k:report['azure'][k] for k in ('uploaded','already_present','download_verified','data_files')},
                          'sql_new_records':report['warehouse']['new_records']},indent=2))
        return 0
    except (DataQualityError,ValueError,RuntimeError,FileNotFoundError) as exc:
        print(f'ADVANCED ETL FAILED: {exc}',file=sys.stderr)
        return 2


if __name__=='__main__':
    sys.exit(main())
