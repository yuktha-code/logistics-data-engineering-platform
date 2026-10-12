"""Individual stages for Airflow DAG tasks, sharing the same validated ETL logic."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from src.pipeline import read_csv_tables, clean_validate, load
from src.spark_events import transform_events_with_spark
from src.mongo_events import mirror_events_to_mongo
from src.synthetic import generate

ROOT=Path(__file__).resolve().parent


def main(argv=None):
    parser=argparse.ArgumentParser(description='Individually orchestratable logistics ETL stages')
    parser.add_argument('stage',choices=['generate','validate','spark','mongo','warehouse','archive'])
    parser.add_argument('--orders',type=int,default=6000)
    parser.add_argument('--raw-dir',type=Path,default=ROOT/'data'/'raw')
    parser.add_argument('--processed-dir',type=Path,default=ROOT/'data'/'processed')
    parser.add_argument('--parquet-dir',type=Path,default=ROOT/'data'/'parquet'/'delivery_events')
    parser.add_argument('--results-dir',type=Path,default=ROOT/'results')
    parser.add_argument('--database-url',default=os.getenv('WAREHOUSE_URL',f'sqlite:///{ROOT / "data" / "warehouse.sqlite"}'))
    parser.add_argument('--mongo-uri',default=os.getenv('MONGO_URI','mongodb://localhost:27018'))
    parser.add_argument('--spark-master',default='local[2]')
    args=parser.parse_args(argv)
    if args.stage=='generate':
        outcome=generate(args.raw_dir,n_orders=args.orders)
    else:
        tables,counts=read_csv_tables(args.raw_dir)
        clean_validate(tables)  # Always check source before optional external writes.
        if args.stage=='validate':
            outcome=counts
        elif args.stage=='spark':
            outcome=transform_events_with_spark(args.raw_dir,args.processed_dir,args.parquet_dir,
                                               counts['events']['distinct'],counts['events']['raw'],args.spark_master)
        elif args.stage=='archive':
            if os.getenv('ENABLE_AZURE_ARCHIVE') != '1':
                outcome = {'status': 'skipped', 'reason': 'ENABLE_AZURE_ARCHIVE is not 1'}
            else:
                from src.azure_archive import upload_to_azure
                outcome = upload_to_azure(args.parquet_dir,
                    os.getenv('AZURE_BLOB_CONTAINER','logistics-archive'),
                    connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
                    account_url=os.getenv('AZURE_STORAGE_ACCOUNT_URL'),
                    prefix=os.getenv('AZURE_BLOB_PREFIX','logistics/events/v1'))
                args.results_dir.mkdir(parents=True,exist_ok=True)
                (args.results_dir/'azure_archive_run.json').write_text(json.dumps(outcome,indent=2)+'\n')
        elif args.stage=='mongo':
            outcome=mirror_events_to_mongo(tables['events'],args.mongo_uri)
        else:
            if not args.processed_dir.is_dir():
                raise FileNotFoundError('Spark-processed source missing: '+str(args.processed_dir))
            outcome=load(args.processed_dir,args.database_url,ROOT/'sql',args.results_dir)
    print(json.dumps(outcome,indent=2,default=str))


if __name__=='__main__':
    main()
