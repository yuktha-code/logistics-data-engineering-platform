"""Optional public Olist CSV adapter; does NOT create invented driver/GPS records."""
from __future__ import annotations
import csv
from collections import OrderedDict
from datetime import datetime,timezone
from hashlib import sha256
from pathlib import Path
from .synthetic import _csv, _stamp


def _load(path:Path):
    if not path.exists():
        raise FileNotFoundError(f"Olist dataset missing: {path.name}")
    with path.open(newline='',encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def _when(value):
    if not value or not value.strip(): return None
    try:
        d=datetime.fromisoformat(value.strip().replace('Z','+00:00'))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f'Olist invalid date {value}') from exc


def _safe_id(s:str):
    return 'LOC_'+sha256(s.encode('utf-8')).hexdigest()[:16]


def convert(olist_dir:Path,raw_dir:Path,limit:int|None=None):
    """Convert public Olist order/customer records into source-compatible tables.

    Events are *derived milestones* from observed date fields, not telematics. All
    GPS coordinates, driver IDs, and shipment revenue remain unknown/blank.
    """
    rows=_load(olist_dir/'olist_orders_dataset.csv')
    customers_raw=_load(olist_dir/'olist_customers_dataset.csv')
    lookup={c['customer_id']:c for c in customers_raw}
    if limit is not None:rows=rows[:limit]
    cities=OrderedDict();customers=OrderedDict();orders=[];events=[]
    for row in rows:
        oid=row['order_id'];cid=row['customer_id'];c=lookup.get(cid)
        if c is None:raise ValueError('Olist orphan customer: '+cid)
        city=(c.get('customer_city') or 'Unknown').strip()
        state=(c.get('customer_state') or 'NA').strip()
        city_id=_safe_id(state+'|'+city)
        cities[city_id]=dict(city_id=city_id,city=city,state=state,latitude='',longitude='')
        customers[cid]=dict(customer_id=cid,city_id=city_id)
        created=_when(row.get('order_purchase_timestamp'))
        if not created:continue
        pickup=_when(row.get('order_delivered_carrier_date'))
        delivered=_when(row.get('order_delivered_customer_date'))
        estimate=_when(row.get('order_estimated_delivery_date'))
        # This field is a date-level estimate: use 23:59:59 to avoid false lateness within the day.
        if estimate is None:
            continue  # Missing SLA deadline: omit rather than invent a delivery promise.
        promised=estimate.replace(hour=23,minute=59,second=59)
        raw_status=row.get('order_status','').strip().lower()
        status='DELIVERED' if raw_status=='delivered' and delivered else ('CANCELLED' if raw_status in ('canceled','unavailable') else 'PENDING')
        if status!='DELIVERED':delivered=None
        orders.append(dict(order_id=oid,customer_id=cid,driver_id='',city_id=city_id,
                           created_at=_stamp(created),picked_up_at=_stamp(pickup),promised_at=_stamp(promised),
                           delivered_at=_stamp(delivered),status=status,distance_km='',fare_inr='',source='olist'))
        steps=[('CREATED',created)]
        if pickup:steps.append(('PICKED_UP',pickup))
        if delivered:steps.append(('DELIVERED',delivered))
        for event_name, date in steps:
            events.append(dict(event_id=_safe_id(oid+'|'+event_name),order_id=oid,event_type=event_name,
              event_at=_stamp(date),ingested_at=_stamp(date),latitude='',longitude='',source='olist_derived_milestone'))
    raw_dir.mkdir(parents=True,exist_ok=True)
    for name,data,columns in [
      ('cities',list(cities.values()),['city_id','city','state','latitude','longitude']),
      ('customers',list(customers.values()),['customer_id','city_id']),
      ('drivers',[],['driver_id','city_id']),
      ('orders',orders,['order_id','customer_id','driver_id','city_id','created_at','picked_up_at','promised_at','delivered_at','status','distance_km','fare_inr','source']),
      ('events',events,['event_id','order_id','event_type','event_at','ingested_at','latitude','longitude','source'])]:
        _csv(raw_dir/f'{name}.csv',columns,data)
    return {"orders":len(orders),"customer_records":len(customers),"cities":len(cities),
            "events_derived":len(events),"source":"olist","driver_gps_available":False}
