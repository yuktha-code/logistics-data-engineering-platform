"""Local reproducible SQLite ETL benchmark (not a Spark/distributed benchmark)."""
from __future__ import annotations
import argparse
import json
import platform
import statistics
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from src.synthetic import generate
from src.pipeline import clean_validate, load, read_csv_tables

ROOT=Path(__file__).resolve().parent


def run_once(order_count: int, seed: int = 42) -> dict:
    with tempfile.TemporaryDirectory(prefix='porter_benchmark_') as folder:
        root=Path(folder)
        raw=root/'raw'
        start=time.perf_counter()
        generate(raw, n_orders=order_count, seed=seed)
        generation=time.perf_counter()-start
        start=time.perf_counter()
        tables, source_counts=read_csv_tables(raw)
        quality=clean_validate(tables)
        validation=time.perf_counter()-start
        url=f'sqlite:///{root / "warehouse.sqlite"}'
        start=time.perf_counter()
        outcome=load(raw,url,ROOT/'sql',root/'results')
        loading=time.perf_counter()-start
        start=time.perf_counter()
        replay=load(raw,url,ROOT/'sql',root/'results')
        replay_seconds=time.perf_counter()-start
        expected_events=source_counts['events']['distinct']
        assert outcome['warehouse_counts']['orders']==order_count
        assert outcome['warehouse_counts']['events']==expected_events
        assert replay['new_records']=={'orders':0,'events':0}
        assert all(quality.values())
        return {'orders':order_count,'raw_events':source_counts['events']['raw'],
                'unique_events':expected_events,
                'generation_seconds':round(generation,4),
                'validation_seconds':round(validation,4),
                'sql_load_seconds':round(loading,4),
                'sql_replay_seconds':round(replay_seconds,4),
                'load_events_per_second':round(expected_events/loading,1),
                'replay_inserted_orders':0,'replay_inserted_events':0}


def main(argv=None):
    p=argparse.ArgumentParser(description='Benchmark synthetic generator and SQLite warehouse')
    p.add_argument('--sizes',default='1000,6000')
    p.add_argument('--runs',type=int,default=2)
    p.add_argument('--output',type=Path,default=ROOT/'results'/'sqlite_benchmark.json')
    args=p.parse_args(argv)
    sizes=[int(x.strip()) for x in args.sizes.split(',')]
    if not sizes or any(x<1 for x in sizes) or not 1<=args.runs<=10:
        p.error('Use positive --sizes and 1 to 10 --runs')
    entries=[]
    for n in sizes:
        samples=[run_once(n,seed=42) for _ in range(args.runs)]
        medians={k:round(statistics.median(s[k] for s in samples),4)
                 for k in ('generation_seconds','validation_seconds',
                           'sql_load_seconds','sql_replay_seconds','load_events_per_second')}
        entries.append({'size_orders':n,'samples':samples,'median':medians})
    report={'environment':{'system':platform.system(),'python':platform.python_version(),
                           'processor':platform.processor()},
            'timestamp_utc':datetime.now(timezone.utc).isoformat(),
            'scope':'local synthetic CSV to SQLite SQL ETL; not distributed/Spark/Mongo/Azure',
            'runs_per_size':args.runs,'results':entries}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'scope':report['scope'],'medians':[
        {'orders':e['size_orders'],**e['median']} for e in entries]},indent=2))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
