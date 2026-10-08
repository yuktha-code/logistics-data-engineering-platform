"""CLI: generate simulated data, or prepare public Olist CSVs, and load warehouse."""
from __future__ import annotations
import argparse
import json
import os
import sys
from pathlib import Path
from src.pipeline import DataQualityError,clean_validate,load,read_csv_tables
from src.synthetic import generate
from src.olist_adapter import convert

ROOT=Path(__file__).resolve().parent

def main(argv=None):
    ap=argparse.ArgumentParser(description='Last-Mile Delivery Reliability ETL')
    ap.add_argument('action',choices=['all','generate','load','check','olist'])
    ap.add_argument('--raw-dir',type=Path,default=ROOT/'data'/'raw')
    ap.add_argument('--results-dir',type=Path,default=ROOT/'results')
    ap.add_argument('--orders',type=int,default=6000)
    ap.add_argument('--seed',type=int,default=42)
    ap.add_argument('--olist-dir',type=Path,default=ROOT/'data'/'olist')
    ap.add_argument('--limit',type=int,default=None)
    ap.add_argument('--database-url',default=os.getenv('WAREHOUSE_URL',f'sqlite:///{ROOT / "data" / "warehouse.sqlite"}'))
    args=ap.parse_args(argv)
    try:
        if args.action in ('all','generate'):
            print('GENERATE: labelled synthetic data', json.dumps(generate(args.raw_dir,args.orders,args.seed)))
        if args.action=='olist':
            print('PREPARE: public Olist historical order data',json.dumps(convert(args.olist_dir,args.raw_dir,args.limit)))
        if args.action=='check':
            tables,counts=read_csv_tables(args.raw_dir)
            print('SOURCE QUALITY:',json.dumps({"checks":clean_validate(tables),"row_counts":counts},indent=2))
        if args.action in ('all','load','olist'):
            print('LOAD: relational warehouse, marts, atomic quality gate')
            summary=load(args.raw_dir,args.database_url,ROOT/'sql',args.results_dir)
            print('QUALITY CHECKS PASSED:',all(summary['quality_checks'].values()))
            print('OVERALL:',json.dumps(summary['overall'],indent=2))
            print('WAREHOUSE:',json.dumps(summary['warehouse_counts']))
            print('NEW RECORDS:',json.dumps(summary['new_records']))
            print('Report:',args.results_dir/'pipeline_report.json')
        return 0
    except (DataQualityError, ValueError, FileNotFoundError) as e:
        print('PIPELINE FAILED QUALITY GATE:',str(e),file=sys.stderr)
        return 2

if __name__=='__main__':sys.exit(main())
