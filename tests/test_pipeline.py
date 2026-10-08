from __future__ import annotations
import csv
import json
import sys
from pathlib import Path
import pytest
from sqlalchemy import text

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from src.synthetic import generate
from src.olist_adapter import convert
from src.pipeline import DataQualityError,engine_for,read_csv_tables,clean_validate,load


def project(tmp_path,n=110):
    raw=tmp_path/'raw';res=tmp_path/'results';db=f'sqlite:///{tmp_path / "warehouse.db"}'
    generate(raw,n_orders=n,seed=42)
    return raw,res,db


def change_csv(path:Path,change):
    with path.open(newline='',encoding='utf-8') as f:
        reader=csv.DictReader(f);headers=reader.fieldnames;rows=list(reader)
    change(rows)
    with path.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=headers);writer.writeheader();writer.writerows(rows)


def test_reproducible_synthetic(tmp_path):
    a=tmp_path/'a';b=tmp_path/'b'
    s1=generate(a,120,11);s2=generate(b,120,11)
    assert s1==s2
    for filename in ['cities.csv','customers.csv','drivers.csv','orders.csv','events.csv']:
        assert (a/filename).read_bytes()==(b/filename).read_bytes()


def test_atomic_warehouse_and_rerun_idempotency(tmp_path):
    raw,res,db=project(tmp_path)
    r1=load(raw,db,ROOT/'sql',res)
    assert r1['warehouse_counts']['orders']==110
    assert r1['new_records']['orders']==110
    assert r1['raw_rows']['events']['identical_duplicates_removed']>0
    assert all(r1['quality_checks'].values())
    r2=load(raw,db,ROOT/'sql',res)
    assert r2['new_records']=={'orders':0,'events':0}
    assert r2['warehouse_counts']==r1['warehouse_counts']
    engine=engine_for(db)
    with engine.connect() as conn:
        assert conn.execute(text('SELECT COUNT(*) FROM pipeline_audit')).scalar_one()==2
        assert conn.execute(text('SELECT COUNT(*) FROM mart_delivery_daily')).scalar_one()>0
    engine.dispose()


def test_incremental_growth_without_double_counting(tmp_path):
    raw,res,db=project(tmp_path,100)
    r1=load(raw,db,ROOT/'sql',res)
    assert r1['warehouse_counts']['orders']==100
    generate(raw,115,42)
    r2=load(raw,db,ROOT/'sql',res)
    assert r2['warehouse_counts']['orders']==115
    assert r2['new_records']['orders']==15
    assert r2['new_records']['events']>0
    r3=load(raw,db,ROOT/'sql',res)
    assert r3['new_records']['orders']==0


def test_invalid_source_fk_rejected_before_write(tmp_path):
    raw,res,db=project(tmp_path)
    change_csv(raw/'events.csv',lambda r:r[0].update(order_id='NOT_A_REAL_ORDER'))
    with pytest.raises(DataQualityError,match='orphan event'):
        load(raw,db,ROOT/'sql',res)
    assert not (tmp_path/'warehouse.db').exists()  # Data never reached DB


def test_conflicting_duplicate_rejected(tmp_path):
    raw,res,db=project(tmp_path)
    change_csv(raw/'orders.csv',lambda rows:rows.append({**rows[0],'status':'FAILED'}))
    with pytest.raises(DataQualityError,match='conflicting duplicate ID'):
        load(raw,db,ROOT/'sql',res)


def test_negative_fare_rejected(tmp_path):
    raw,res,db=project(tmp_path)
    change_csv(raw/'orders.csv',lambda rows:rows[0].update(fare_inr='-500'))
    with pytest.raises(DataQualityError,match='negative distance/fare'):
        load(raw,db,ROOT/'sql',res)


def test_delivery_time_order_is_validated(tmp_path):
    raw,res,db=project(tmp_path)
    change_csv(raw/'orders.csv',lambda rows:rows[0].update(promised_at='2025-01-01T00:00:00Z'))
    with pytest.raises(DataQualityError,match='promise before order'):
        load(raw,db,ROOT/'sql',res)


def test_mart_denominator_excludes_failed_or_cancelled(tmp_path):
    raw,res,db=project(tmp_path,150)
    result=load(raw,db,ROOT/'sql',res)
    engine=engine_for(db)
    with engine.connect() as conn:
        n=conn.execute(text("SELECT COUNT(*) FROM fact_delivery WHERE status='DELIVERED'")).scalar_one()
        on_time=conn.execute(text("SELECT COUNT(*) FROM fact_delivery WHERE status='DELIVERED' AND on_time=1")).scalar_one()
        exc=conn.execute(text("SELECT COUNT(*) FROM mart_delivery_exceptions")).scalar_one()
        total=conn.execute(text('SELECT COUNT(*) FROM fact_delivery')).scalar_one()
    engine.dispose()
    assert n<total
    assert exc>=total-n
    assert result['overall']['on_time_pct']==round(100*on_time/n,2)


def test_olist_order_conversion_without_fake_driver_data(tmp_path):
    indir=tmp_path/'olist';indir.mkdir();raw=tmp_path/'raw';res=tmp_path/'res'
    with (indir/'olist_customers_dataset.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['customer_id','customer_city','customer_state']);w.writeheader()
        w.writerow(dict(customer_id='cust1',customer_city='sao paulo',customer_state='SP'))
    with (indir/'olist_orders_dataset.csv').open('w',newline='') as f:
        cols=['order_id','customer_id','order_status','order_purchase_timestamp','order_delivered_carrier_date',
        'order_delivered_customer_date','order_estimated_delivery_date']
        w=csv.DictWriter(f,fieldnames=cols);w.writeheader()
        w.writerow(dict(order_id='order1',customer_id='cust1',order_status='delivered',
          order_purchase_timestamp='2018-01-01 10:00:00',order_delivered_carrier_date='2018-01-02 10:00:00',
          order_delivered_customer_date='2018-01-05 14:00:00',order_estimated_delivery_date='2018-01-06 00:00:00'))
    result=convert(indir,raw)
    assert result['driver_gps_available'] is False
    tables,counts=read_csv_tables(raw)
    assert clean_validate(tables)['source_foreign_keys_valid']
    assert all(e['latitude']=='' for e in tables['events'])
    assert all(o['driver_id']=='' for o in tables['orders'])
    db=f'sqlite:///{tmp_path / "o.db"}'
    output=load(raw,db,ROOT/'sql',res)
    assert output['overall']['delivered']==1
    assert output['overall']['on_time_pct']==100.0


def test_results_report_serializable(tmp_path):
    raw,res,db=project(tmp_path)
    load(raw,db,ROOT/'sql',res)
    p=json.loads((res/'pipeline_report.json').read_text())
    assert p['overall']['total_orders']==110
    assert all(json.loads((res/'quality_checks.json').read_text()).values())
    assert 'DATA PROVENANCE' in (res/'delivery_dashboard.html').read_text()


def test_correction_upserts_existing_row(tmp_path):
    raw,res,db=project(tmp_path,120)
    r1=load(raw,db,ROOT/'sql',res)
    def later_delivery(rows):
        delivered=next(r for r in rows if r['status']=='DELIVERED')
        # Legal correction that should propagate on rerun without changing row count.
        import datetime as dt
        new=dt.datetime.fromisoformat(delivered['promised_at'].replace('Z','+00:00'))+dt.timedelta(hours=4)
        delivered['delivered_at']=new.strftime('%Y-%m-%dT%H:%M:%SZ')
        later_delivery.id=delivered['order_id']
    change_csv(raw/'orders.csv',later_delivery)
    # This test is specifically about warehouse corrections; source events are older but valid.
    r2=load(raw,db,ROOT/'sql',res)
    assert r2['new_records']['orders']==0
    engine=engine_for(db)
    with engine.connect() as conn:
        current=conn.execute(text('SELECT on_time,delay_minutes FROM fact_delivery WHERE order_id=:id'),{'id':later_delivery.id}).one()
        assert current.on_time==0
        assert current.delay_minutes==240.0
    engine.dispose()
