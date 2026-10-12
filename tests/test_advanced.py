"""Offline unit/contract tests, run without MongoDB or PySpark installed."""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from src.mongo_events import chunks, event_document
from src.pipeline import DataQualityError
from src.synthetic import generate
from run_advanced import execute


def test_event_document_uses_stable_mongo_id_and_original_values():
    row=dict(event_id='E001',order_id='O001',event_type='CREATED',
             event_at='2026-01-01T00:00:00Z',ingested_at='2026-01-01T00:02:00Z',
             latitude='',longitude='',source='public_olist')
    doc=event_document(row)
    assert doc['_id']=='E001'
    assert doc['latitude']==''
    assert doc['source']=='public_olist'
    assert len(doc)==len(row)+1


def test_event_document_rejects_missing_fields():
    with pytest.raises(ValueError,match='missing fields'):
        event_document({'event_id':'1','order_id':'2'})


def test_event_document_rejects_blank_identifiers():
    row=dict(event_id='',order_id='O',event_type='CREATED',
             event_at='2026-01-01T00:00:00Z',ingested_at='2026-01-01T00:00:00Z',
             latitude='',longitude='',source='synthetic')
    with pytest.raises(ValueError,match='populated'):
        event_document(row)


def test_batch_iterator_never_drops_remainder():
    assert [len(x) for x in chunks(({'id':i} for i in range(12)),5)]==[5,5,2]
    assert list(chunks([],5))==[]
    with pytest.raises(ValueError,match='positive'):
        list(chunks([],0))


def test_source_failure_prevents_external_side_effects(tmp_path,monkeypatch):
    raw=tmp_path/'raw';generate(raw,15)
    path=raw/'events.csv'
    with path.open(newline='',encoding='utf-8') as f:
        rd=csv.DictReader(f);fields=rd.fieldnames;rows=list(rd)
    rows[0]['order_id']='MISSING_ORDER'
    with path.open('w',newline='',encoding='utf-8') as f:
        wr=csv.DictWriter(f,fieldnames=fields);wr.writeheader();wr.writerows(rows)
    import run_advanced
    def should_never_call(*args,**kw):
        raise AssertionError('Source validation should prevent all writes')
    monkeypatch.setattr(run_advanced,'mirror_events_to_mongo',should_never_call)
    monkeypatch.setattr(run_advanced,'transform_events_with_spark',should_never_call)
    with pytest.raises(DataQualityError,match='orphan event'):
        execute(raw,f'sqlite:///{tmp_path/"data.db"}',tmp_path/'out',tmp_path/'processed',
                tmp_path/'archive','mongodb://unused')
    assert not (tmp_path/'data.db').exists()


def test_advanced_orchestration_passes_cleaned_spark_events_into_sql(tmp_path,monkeypatch):
    import shutil
    import run_advanced
    raw=tmp_path/'raw';generate(raw,22)
    calls=[]
    def fake_spark(src,dest,archive,expected_unique,expected_raw,master):
        calls.append(('spark',expected_unique,expected_raw))
        shutil.copytree(src,dest,dirs_exist_ok=True)
        return {'input_events':expected_raw,'deduplicated_events':expected_unique}
    def fake_mongo(events,mongo_uri):
        calls.append(('mongo',len(events)))
        return {'input_events':len(events),'upserted':len(events)}
    monkeypatch.setattr(run_advanced,'transform_events_with_spark',fake_spark)
    monkeypatch.setattr(run_advanced,'mirror_events_to_mongo',fake_mongo)
    db=f'sqlite:///{tmp_path/"warehouse.db"}'
    args=(raw,db,tmp_path/'res',tmp_path/'processed',tmp_path/'archive','mongodb://unused')
    r1=execute(*args)
    assert r1['warehouse']['warehouse_counts']['orders']==22
    assert r1['warehouse']['new_records']['orders']==22
    assert [call[0] for call in calls]==['spark','mongo']
    assert calls[0][1]==calls[1][1]
    assert json.loads((tmp_path/'res'/'advanced_run.json').read_text())['mongo']['input_events']>0
    r2=execute(*args)
    assert r2['warehouse']['new_records']=={'orders':0,'events':0}


def test_advanced_optional_modes_do_not_import_missing_heavy_deps(tmp_path,monkeypatch):
    import run_advanced
    raw=tmp_path/'raw';generate(raw,18)
    monkeypatch.setattr(run_advanced,'mirror_events_to_mongo',lambda e,u:{'input_events':len(e)})
    report=execute(raw,f'sqlite:///{tmp_path/"w.db"}',tmp_path/'res',tmp_path/'processed',
                   tmp_path/'archive','mongodb://unused',use_spark=False)
    assert report['spark'] is None
    assert report['warehouse']['warehouse_counts']['orders']==18


def test_advanced_rejects_cloud_archive_without_spark(tmp_path):
    raw=tmp_path/'raw';generate(raw,10)
    with pytest.raises(ValueError,match='requires'):
        execute(raw,f'sqlite:///{tmp_path/"w.db"}',tmp_path/'res',
                tmp_path/'processed',tmp_path/'archive','mongodb://unused',
                use_mongo=False,use_spark=False,azure_archive=True)
    assert not (tmp_path/'w.db').exists()


def test_advanced_cloud_archive_is_optional_and_runs_after_sql(tmp_path,monkeypatch):
    import shutil
    import run_advanced
    import src.azure_archive as archiver
    raw=tmp_path/'raw';generate(raw,12)
    observed=[]
    def fake_spark(src,dest,archive,expected_unique,expected_raw,master):
        shutil.copytree(src,dest,dirs_exist_ok=True)
        archive.mkdir(parents=True,exist_ok=True)
        return {'input_events':expected_raw,'deduplicated_events':expected_unique}
    def fake_archive(directory, container, **kwargs):
        observed.append(('archive',container,Path(directory).is_dir()))
        assert (tmp_path/'w.db').exists(), 'Warehouse should commit before archiving'
        return {'uploaded':1,'already_present':0,'data_files':1,'download_verified':1}
    monkeypatch.setattr(run_advanced,'transform_events_with_spark',fake_spark)
    monkeypatch.setattr(archiver,'upload_to_azure',fake_archive)
    report=execute(raw,f'sqlite:///{tmp_path/"w.db"}',tmp_path/'res',
                   tmp_path/'processed',tmp_path/'archive','mongodb://unused',
                   use_mongo=False,use_spark=True,azure_archive=True)
    assert report['azure']['uploaded']==1
    assert observed==[('archive','logistics-archive',True)]
