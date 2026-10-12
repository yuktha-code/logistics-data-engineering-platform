"""Azure archive contract tests with an in-memory blob API double.

These check checksum/replay/publish logic; they are NOT Azure service tests.
"""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.azure_archive import (
    ArchiveIntegrityError, parquet_inventory, sync_parquet_archive,
    upload_to_azure, digest_file,
)


class FakeDownload:
    def __init__(self, payload: bytes):
        self.payload = payload

    def chunks(self):
        for start in range(0, len(self.payload), 7):
            yield self.payload[start:start + 7]


class FakeBlob:
    def __init__(self, storage: dict, key: str):
        self.storage, self.key = storage, key

    def exists(self):
        return self.key in self.storage

    def get_blob_properties(self):
        entry = self.storage[self.key]
        return SimpleNamespace(size=len(entry['data']), metadata=entry['metadata'])

    def upload_blob(self, content, *, overwrite=False, metadata=None, content_type=None):
        if self.exists() and not overwrite:
            raise FileExistsError(self.key)
        payload = content.read() if hasattr(content,'read') else content
        self.storage[self.key] = {'data': payload, 'metadata': metadata or {}}

    def download_blob(self):
        return FakeDownload(self.storage[self.key]['data'])


class FakeContainer:
    container_name = 'fake-archive'

    def __init__(self):
        self.storage = {}

    def get_blob_client(self, name):
        return FakeBlob(self.storage, name)


@pytest.fixture
def mock_parquet(tmp_path):
    root = tmp_path/'archive'
    a = root/'city_id=BLR'/'event_month=2026-01'
    b = root/'city_id=HYD'/'event_month=2026-02'
    a.mkdir(parents=True)
    b.mkdir(parents=True)
    (a/'part-a.parquet').write_bytes(b'PAR1bytes-from-fake-blrPAR1')
    (b/'part-b.parquet').write_bytes(b'PAR1bytes-from-fake-hydPAR1')
    (root/'_SUCCESS').touch()
    return root


def test_inventory_is_content_addressed_and_partition_safe(mock_parquet):
    inv=parquet_inventory(mock_parquet)
    assert len(inv)==2
    assert all('/city_id=' in r['blob_name'] for r in inv)
    assert all(r['blob_name'].endswith('/'+r['sha256']+'.parquet') for r in inv)
    assert inv[0]['sha256']==digest_file(Path(inv[0]['local_path']))
    assert sum(r['bytes'] for r in inv)>0


def test_archive_upload_readback_manifest_and_replay(mock_parquet):
    client=FakeContainer()
    first=sync_parquet_archive(mock_parquet,client)
    assert first['uploaded']==2 and first['already_present']==0
    assert first['download_verified']==2
    assert len(client.storage)==3   # two objects, one manifest
    manifest=json.loads(client.storage[first['manifest_blob']]['data'])
    assert manifest['version']==1
    assert len(manifest['files'])==2
    second=sync_parquet_archive(mock_parquet,client)
    assert second['uploaded']==0 and second['already_present']==2
    assert second['download_verified']==2
    assert len(client.storage)==3


def test_blob_collision_fails_before_new_manifest(mock_parquet):
    client=FakeContainer()
    first=sync_parquet_archive(mock_parquet,client)
    first_key=first['files'][0]['blob_name']
    previous_manifest=client.storage[first['manifest_blob']]['data']
    client.storage[first_key]['metadata']['sha256']='bad'
    with pytest.raises(ArchiveIntegrityError,match='Existing object'):
        sync_parquet_archive(mock_parquet,client)
    assert client.storage[first['manifest_blob']]['data']==previous_manifest


def test_download_mismatch_blocks_manifest(mock_parquet):
    client=FakeContainer()
    real_get=client.get_blob_client
    class BadDownloadBlob(FakeBlob):
        def download_blob(self):
            return FakeDownload(b'modified payload')
    def hacked_get(name):
        blob=real_get(name)
        if name.endswith('.parquet'):
            return BadDownloadBlob(blob.storage,blob.key)
        return blob
    client.get_blob_client=hacked_get
    with pytest.raises(ArchiveIntegrityError,match='Download checksum mismatch'):
        sync_parquet_archive(mock_parquet,client)
    assert not any(k.endswith('/_manifest.json') for k in client.storage)


def test_archive_rejects_missing_and_invalid_inputs(tmp_path,mock_parquet):
    with pytest.raises(FileNotFoundError):
        parquet_inventory(tmp_path/'missing')
    with pytest.raises(ValueError,match='prefix'):
        parquet_inventory(mock_parquet,prefix='../escape')
    stray=mock_parquet/'not-a-partition'
    stray.mkdir()
    (stray/'file.parquet').write_bytes(b'hi')
    with pytest.raises(ValueError,match='partition'):
        parquet_inventory(mock_parquet)


def test_cloud_auth_requires_configuration(monkeypatch,mock_parquet):
    # No SDK installed in offline runner, so stub just the module-level factory.
    import src.azure_archive as module
    def factory(conn, account):
        if not (conn or account):
            raise ValueError('missing cloud configuration')
        return None
    monkeypatch.setattr(module,'azure_client',factory)
    with pytest.raises(ValueError,match='configuration'):
        upload_to_azure(mock_parquet,'logistics-archive')
