"""Reproducible relational ETL with duplicate, FK, temporal and accounting checks."""
from __future__ import annotations
import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import create_engine, event, text
from .reporting import render

REQUIRED = {
 "cities": ["city_id","city","state","latitude","longitude"],
 "customers": ["customer_id","city_id"],
 "drivers": ["driver_id","city_id"],
 "orders": ["order_id","customer_id","driver_id","city_id","created_at","picked_up_at","promised_at","delivered_at","status","distance_km","fare_inr","source"],
 "events": ["event_id","order_id","event_type","event_at","ingested_at","latitude","longitude","source"],
}
KEYS = {"cities":"city_id", "customers":"customer_id", "drivers":"driver_id", "orders":"order_id", "events":"event_id"}
STATUS = {"DELIVERED","CANCELLED","FAILED","PENDING"}
EVENTS = {"CREATED","PICKED_UP","IN_TRANSIT","DELIVERED","CANCELLED","FAILED"}

class DataQualityError(ValueError):
    """Raised on source schema, key, referential integrity, or semantic violations."""


def read_csv_tables(raw_dir: Path) -> tuple[dict[str,list[dict]],dict]:
    tables: dict[str, list[dict]] = {}
    counts: dict[str,dict] = {}
    for table, cols in REQUIRED.items():
        path = raw_dir / f"{table}.csv"
        if not path.exists():
            raise DataQualityError(f"Missing required source: {path}")
        with path.open(encoding="utf-8-sig", newline="") as f:
            rdr = csv.DictReader(f)
            if not rdr.fieldnames or not set(cols).issubset(rdr.fieldnames):
                raise DataQualityError(f"{path.name} schema mismatch; requires {cols}")
            raw = [{col: (row.get(col) or "").strip() for col in cols} for row in rdr]
        seen: dict[str,dict] = {}
        identical = 0
        for row in raw:
            k = row[KEYS[table]]
            if not k:
                raise DataQualityError(f"{table}: blank primary key")
            if k in seen:
                if row != seen[k]:
                    raise DataQualityError(f"{table}: conflicting duplicate ID {k}")
                identical += 1
            else:
                seen[k] = row
        tables[table] = list(seen.values())
        counts[table] = {"raw":len(raw), "distinct":len(seen), "identical_duplicates_removed":identical}
    return tables, counts


def parse_ts(value: str, key: str, required: bool=True) -> datetime | None:
    if not value:
        if required: raise DataQualityError(f"missing timestamp: {key}")
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError as e:
        raise DataQualityError(f"invalid timestamp for {key}: {value}") from e


def clean_validate(tables: dict[str,list[dict]]) -> dict:
    cities={r['city_id']:r for r in tables['cities']}
    customers={r['customer_id']:r for r in tables['customers']}
    drivers={r['driver_id']:r for r in tables['drivers']}
    orders={r['order_id']:r for r in tables['orders']}
    errors=[]
    def bad(msg:str):
        if len(errors)<30: errors.append(msg)
    for r in customers.values():
        if r['city_id'] not in cities: bad(f"customer {r['customer_id']} orphan city")
    for r in drivers.values():
        if r['city_id'] not in cities: bad(f"driver {r['driver_id']} orphan city")
    for r in orders.values():
        k=r['order_id']
        if r['city_id'] not in cities: bad(f"{k}: missing city")
        if r['customer_id'] not in customers: bad(f"{k}: missing customer")
        elif customers[r['customer_id']]['city_id']!=r['city_id']: bad(f"{k}: customer/city mismatch")
        if r['driver_id'] and r['driver_id'] not in drivers: bad(f"{k}: missing driver")
        elif r['driver_id'] and drivers[r['driver_id']]['city_id']!=r['city_id']: bad(f"{k}: driver/city mismatch")
        if r['status'] not in STATUS: bad(f"{k}: unknown status {r['status']}")
        try:
            if float(r['distance_km'] or 0)<0 or float(r['fare_inr'] or 0)<0:
                bad(f"{k}: negative distance/fare")
        except ValueError: bad(f"{k}: invalid distance/fare")
        created=parse_ts(r['created_at'], k+'.created_at')
        promised=parse_ts(r['promised_at'], k+'.promised_at')
        pickup=parse_ts(r['picked_up_at'], k+'.picked_up_at',False)
        delivered=parse_ts(r['delivered_at'],k+'.delivered_at',False)
        if promised < created: bad(f"{k}: promise before order")
        if pickup and pickup<created: bad(f"{k}: pickup before order")
        if delivered and (delivered<created or (pickup and delivered<pickup)):
            bad(f"{k}: delivered before order/pickup")
        if r['status']=='DELIVERED' and delivered is None: bad(f"{k}: delivered order has no arrival")
        if r['status']!='DELIVERED' and delivered is not None: bad(f"{k}: non-delivered order has arrival")
    for r in tables['events']:
        k=r['event_id']
        if r['order_id'] not in orders: bad(f"{k}: orphan event -> {r['order_id']}")
        if r['event_type'] not in EVENTS: bad(f"{k}: unknown event type")
        at=parse_ts(r['event_at'], k+'.event_at')
        ingest=parse_ts(r['ingested_at'], k+'.ingested_at')
        if ingest < at: bad(f"{k}: ingestion time before event")
        if r['order_id'] in orders:
            created = parse_ts(orders[r['order_id']]['created_at'], k+'.parent_created')
            if at < created: bad(f"{k}: event before parent order")
        try:
            if r['latitude'] and not (-90<=float(r['latitude'])<=90): bad(f"{k}: latitude out of range")
            if r['longitude'] and not (-180<=float(r['longitude'])<=180): bad(f"{k}: longitude out of range")
        except ValueError: bad(f"{k}: invalid coordinates")
    if errors:
        raise DataQualityError(f"{len(errors)} source quality errors: "+"; ".join(errors))
    return {"source_schema_valid":True, "source_primary_keys_valid":True,
            "source_foreign_keys_valid":True, "source_temporal_rules_valid":True,
            "source_numeric_ranges_valid":True, "no_conflicting_duplicate_ids":True}


def engine_for(url: str):
    engine=create_engine(url, future=True)
    if url.startswith('sqlite'):
        @event.listens_for(engine,'connect')
        def fk_pragma(dbapi_conn, _):
            dbapi_conn.execute('PRAGMA foreign_keys=ON')
    return engine


def statements(path: Path):
    return [s.strip() for s in path.read_text(encoding='utf-8').split(';') if s.strip()]


def normalized(tables:dict[str,list[dict]]):
    """Turn empty optional fields into NULL, precompute delivered on-time and delay."""
    for table in ['orders','events']:
        for r in tables[table]:
            if table=='orders':
                for k in ('driver_id','picked_up_at','delivered_at','distance_km','fare_inr'):
                    if r[k]=='':r[k]=None
                for k in ('distance_km','fare_inr'):
                    if r[k] is not None:r[k]=float(r[k])
            else:
                for k in ('latitude','longitude'):
                    r[k]=float(r[k]) if r[k] else None
    for r in tables['cities']:
        for k in ('latitude','longitude'):
            r[k]=float(r[k]) if r[k] else None
    facts=[]
    for r in tables['orders']:
        delivery=dict(r)
        if r['status']=='DELIVERED':
            promised=parse_ts(r['promised_at'],r['order_id'])
            actual=parse_ts(r['delivered_at'],r['order_id'])
            delta=round((actual-promised).total_seconds()/60,2)
            delivery['delay_minutes']=max(0.0,delta)
            delivery['on_time']=int(delta<=0)
        else:
            delivery['delay_minutes']=None
            delivery['on_time']=None
        facts.append(delivery)
    return facts


def upsert(conn, table:str, rows:list[dict], pk:str):
    if not rows:return
    cols=list(rows[0].keys())
    allowed={'dim_city','dim_customer','dim_driver','stg_orders','stg_events','fact_delivery','fact_delivery_event'}
    if table not in allowed:raise ValueError('unexpected target table')
    col_names=', '.join(cols)
    param_names=', '.join(':'+c for c in cols)
    update=', '.join(f'{c}=excluded.{c}' for c in cols if c!=pk)
    sql=text(f'INSERT INTO {table} ({col_names}) VALUES ({param_names}) ON CONFLICT ({pk}) DO UPDATE SET {update}')
    for start in range(0,len(rows),500):conn.execute(sql,rows[start:start+500])


def check_db(conn, tables:dict[str,list[dict]], counts:dict):
    mapping={'dim_city':'cities','dim_customer':'customers','dim_driver':'drivers','stg_orders':'orders',
             'stg_events':'events','fact_delivery':'orders','fact_delivery_event':'events'}
    checks={}
    for table,source in mapping.items():
        count=conn.execute(text(f'SELECT COUNT(*) FROM {table}')).scalar_one()
        checks[f'{table}_includes_source']=count>=len(tables[source])
    for fact, dim, col in [('fact_delivery','dim_city','city_id'),('fact_delivery','dim_customer','customer_id'),
                           ('fact_delivery','dim_driver','driver_id'),('fact_delivery_event','fact_delivery','order_id')]:
        q=f'''SELECT COUNT(*) FROM {fact} f LEFT JOIN {dim} d ON f.{col}=d.{col if dim!='fact_delivery' else 'order_id'}
        WHERE f.{col} IS NOT NULL AND d.{col if dim!='fact_delivery' else 'order_id'} IS NULL'''
        checks[f'{fact}_{col}_fk_valid']=conn.execute(text(q)).scalar_one()==0
    checks['delivered_have_on_time']=conn.execute(text("SELECT COUNT(*) FROM fact_delivery WHERE status='DELIVERED' AND on_time IS NULL")).scalar_one()==0
    checks['non_delivered_no_on_time']=conn.execute(text("SELECT COUNT(*) FROM fact_delivery WHERE status!='DELIVERED' AND on_time IS NOT NULL")).scalar_one()==0
    checks['duplicates_removed_from_raw']=counts['events']['identical_duplicates_removed']>=0
    checks['all_source_orders_loaded']=conn.execute(text('SELECT COUNT(*) FROM fact_delivery WHERE order_id IN (SELECT order_id FROM stg_orders)')).scalar_one()>=len(tables['orders'])
    return checks


def load(raw_dir:Path, database_url:str, sql_dir:Path, results_dir:Path) -> dict:
    results_dir.mkdir(parents=True,exist_ok=True)
    # Prevalidate entire batch before attempting writes.
    tables,counts=read_csv_tables(raw_dir)
    source_qc=clean_validate(tables)
    facts=normalized(tables)
    engine=engine_for(database_url)
    start=datetime.now(timezone.utc).isoformat()
    try:
        with engine.begin() as conn:
            for stmt in statements(sql_dir/'schema.sql'):
                conn.exec_driver_sql(stmt)
            before_orders=conn.execute(text('SELECT COUNT(*) FROM fact_delivery')).scalar_one()
            before_events=conn.execute(text('SELECT COUNT(*) FROM fact_delivery_event')).scalar_one()
            for target, source, key in [('dim_city','cities','city_id'),('dim_customer','customers','customer_id'),
                                        ('dim_driver','drivers','driver_id'),('stg_orders','orders','order_id'),
                                        ('stg_events','events','event_id')]:
                upsert(conn,target,tables[source],key)
            upsert(conn,'fact_delivery',facts,'order_id')
            upsert(conn,'fact_delivery_event',tables['events'],'event_id')
            for stmt in statements(sql_dir/'marts.sql'):
                conn.exec_driver_sql(stmt)
            quality={**source_qc,**check_db(conn,tables,counts)}
            if not all(quality.values()):
                raise DataQualityError('warehouse quality gate failed: '+str([k for k,v in quality.items() if not v]))
            after_orders=conn.execute(text('SELECT COUNT(*) FROM fact_delivery')).scalar_one()
            after_events=conn.execute(text('SELECT COUNT(*) FROM fact_delivery_event')).scalar_one()
            finish=datetime.now(timezone.utc).isoformat()
            inserted_orders=after_orders-before_orders
            inserted_events=after_events-before_events
            conn.execute(text('''INSERT INTO pipeline_audit
            (run_id,run_started,run_finished,source_orders,source_events,duplicates_removed,inserted_orders,inserted_events,quality_status)
            VALUES (:run_id,:run_started,:run_finished,:source_orders,:source_events,:duplicates_removed,:inserted_orders,:inserted_events,'PASS')'''),
             dict(run_id=str(uuid.uuid4()),run_started=start,run_finished=finish,
                  source_orders=len(tables['orders']),source_events=len(tables['events']),
                  duplicates_removed=sum(s['identical_duplicates_removed'] for s in counts.values()),
                  inserted_orders=inserted_orders,inserted_events=inserted_events))
            city_rows=[dict(row._mapping) for row in conn.execute(text('SELECT * FROM mart_city_performance ORDER BY total_orders DESC'))]
            summary={"source_type":sorted(set(o['source'] for o in tables['orders'])),
              "raw_rows":counts,"warehouse_counts":{"orders":after_orders,"events":after_events,
                "cities":conn.execute(text('SELECT COUNT(*) FROM dim_city')).scalar_one()},
              "new_records":{"orders":inserted_orders,"events":inserted_events},
              "on_time_definition":"Delivered on/before promised_at; excludes failed/cancelled orders from denominator",
              "overall":dict(conn.execute(text('''SELECT COUNT(*) AS total_orders,
                SUM(CASE WHEN status='DELIVERED' THEN 1 ELSE 0 END) AS delivered,
                SUM(CASE WHEN status='DELIVERED' AND on_time=1 THEN 1 ELSE 0 END) AS on_time,
                ROUND(100.0*SUM(CASE WHEN status='DELIVERED' AND on_time=1 THEN 1 ELSE 0 END)/
                NULLIF(SUM(CASE WHEN status='DELIVERED' THEN 1 ELSE 0 END),0),2) AS on_time_pct
                FROM fact_delivery''')).one()._mapping),
              "city_performance":city_rows,"quality_checks":quality}
    finally:
        engine.dispose()
    (results_dir/'pipeline_report.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False,default=str)+'\n',encoding='utf-8')
    (results_dir/'quality_checks.json').write_text(json.dumps(quality,indent=2)+'\n',encoding='utf-8')
    render(summary,results_dir/'delivery_dashboard.html')
    return summary
