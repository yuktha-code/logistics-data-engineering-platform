"""Opt-in real SDK + Azurite emulator test. Does NOT prove actual Azure cloud."""
from __future__ import annotations
import os
import pytest

pytestmark = pytest.mark.skipif(os.getenv('RUN_AZURE_EMULATOR_INTEGRATION') != '1',
                              reason='requires Azure Blob SDK and running Azurite emulator')


def test_azurite_upload_download_replay(tmp_path):
    from azure.storage.blob import BlobServiceClient
    from src.azure_archive import upload_to_azure
    connection=os.environ['AZURE_STORAGE_CONNECTION_STRING']
    service=BlobServiceClient.from_connection_string(connection)
    container='porter-azure-integration'
    with service:
        client=service.get_container_client(container)
        if not client.exists():
            client.create_container()
    a=tmp_path/'city_id=HYD'/'event_month=2026-01'
    a.mkdir(parents=True)
    (a/'part-test.parquet').write_bytes(b'PAR1test-payloadPAR1')
    first=upload_to_azure(tmp_path,container,connection_string=connection,prefix='ci/check')
    assert first['uploaded']==1 and first['download_verified']==1
    replay=upload_to_azure(tmp_path,container,connection_string=connection,prefix='ci/check')
    assert replay['uploaded']==0 and replay['already_present']==1
