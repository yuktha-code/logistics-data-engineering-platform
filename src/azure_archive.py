"""Verified Azure Blob archive for Spark-generated Parquet partitions.

Uses append-only content-addressed blob names to make identical archive runs
replay-safe even if Spark changes its part-*.parquet file names. A manifest is
published LAST, after every data blob has been checked. No cloud credentials are
stored in source code or written to the manifest.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class ArchiveIntegrityError(RuntimeError):
    """An existing or newly uploaded Azure blob did not match the local bytes."""


def digest_file(path: Path, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def parquet_inventory(directory: Path, prefix: str = 'logistics/events/v1') -> list[dict[str, Any]]:
    """Inventory only actual Parquet data files under a fixed directory root."""
    root = Path(directory)
    if not root.is_dir():
        raise FileNotFoundError(f'Parquet directory missing: {root}')
    normalized_prefix = prefix.strip('/')
    if not normalized_prefix or any(x in ('.', '..') for x in normalized_prefix.split('/')):
        raise ValueError('Archive prefix must be a nonempty safe blob prefix')
    inventory = []
    for path in sorted(root.rglob('*.parquet')):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        # Only accept the expected two partition segments; don't accidentally
        # archive notebooks, debug folders, or credentials under the root.
        parts = relative.parts
        if len(parts) != 3 or not parts[0].startswith('city_id=') or not parts[1].startswith('event_month='):
            raise ValueError(f'Unexpected Parquet partition layout: {relative}')
        sha = digest_file(path)
        blob = f'{normalized_prefix}/{parts[0]}/{parts[1]}/{sha}.parquet'
        inventory.append({'local_path': str(path), 'relative_path': relative.as_posix(),
                          'blob_name': blob, 'sha256': sha, 'bytes': path.stat().st_size})
    if not inventory:
        raise ValueError('No Parquet data files found; archive aborted')
    return inventory


def _hash_remote(blob_client: Any) -> str:
    digest = hashlib.sha256()
    stream = blob_client.download_blob()
    for block in stream.chunks():
        digest.update(block)
    return digest.hexdigest()


def sync_parquet_archive(directory: Path, container_client: Any,
                         prefix: str = 'logistics/events/v1',
                         verify_download: bool = True) -> dict[str, Any]:
    """Archive Parquet files using an Azure ContainerClient or compatible test fake.

    Existing blobs are only skipped when their size and sha256 metadata agree.
    All new uploads are hashed again by download unless verify_download=False.
    Replays do not create new data blobs; a versionless latest-manifest key is
    overwritten after all file-level checks pass. This is not an atomic multi-
    blob commit: consumers must use the published manifest, not prefix listing.
    """
    inventory = parquet_inventory(directory, prefix)
    stats = {'data_files': len(inventory), 'bytes': sum(i['bytes'] for i in inventory),
             'uploaded': 0, 'already_present': 0, 'download_verified': 0,
             'container': getattr(container_client, 'container_name', 'configured-container'),
             'prefix': prefix.strip('/'), 'files': []}
    for item in inventory:
        blob = container_client.get_blob_client(item['blob_name'])
        if blob.exists():
            props = blob.get_blob_properties()
            metadata = getattr(props, 'metadata', {}) or {}
            if (str(metadata.get('sha256', '')) != item['sha256']
                    or int(getattr(props, 'size', -1)) != item['bytes']):
                raise ArchiveIntegrityError('Existing object does not match manifest: '+item['blob_name'])
            stats['already_present'] += 1
        else:
            with open(item['local_path'], 'rb') as content:
                blob.upload_blob(content, overwrite=False,
                                 metadata={'sha256': item['sha256']})
            stats['uploaded'] += 1
        if verify_download:
            if _hash_remote(blob) != item['sha256']:
                raise ArchiveIntegrityError('Download checksum mismatch: '+item['blob_name'])
            stats['download_verified'] += 1
        stats['files'].append({k: item[k] for k in ('relative_path', 'blob_name', 'sha256', 'bytes')})

    # Publish manifest last: a consumer can use it to resolve the current set.
    manifest = json.dumps({'version': 1, 'prefix': stats['prefix'], 'files': stats['files']},
                          sort_keys=True, indent=2).encode('utf-8')
    manifest_key = f"{stats['prefix']}/_manifest.json"
    client = container_client.get_blob_client(manifest_key)
    client.upload_blob(manifest, overwrite=True)
    if verify_download:
        if _hash_remote(client) != hashlib.sha256(manifest).hexdigest():
            raise ArchiveIntegrityError('Published Azure manifest did not pass read-back verification')
    stats['manifest_blob'] = manifest_key
    return stats


def azure_client(connection_string: str | None = None,
                 account_url: str | None = None) -> Any:
    """Return an SDK BlobServiceClient with credentials never logged or stored."""
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise RuntimeError('Install requirements-cloud.txt for Azure Blob support') from exc
    if connection_string:
        # The storage emulator can lag behind the latest Azure SDK service API.
        # Use a known-compatible API only for Azurite; keep real Azure defaults.
        if connection_string.strip().lower().startswith('usedevelopmentstorage=true'):
            return BlobServiceClient.from_connection_string(connection_string, api_version='2023-11-03')
        return BlobServiceClient.from_connection_string(connection_string)
    if account_url:
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError as exc:
            raise RuntimeError('Install azure-identity for passwordless Azure auth') from exc
        return BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
    raise ValueError('Specify AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL')


def upload_to_azure(directory: Path, container_name: str, *,
                    connection_string: str | None = None,
                    account_url: str | None = None,
                    prefix: str = 'logistics/events/v1',
                    verify_download: bool = True) -> dict[str, Any]:
    if not container_name:
        raise ValueError('Azure Blob container name is required')
    service = azure_client(connection_string, account_url)
    with service:
        client = service.get_container_client(container_name)
        return sync_parquet_archive(directory, client, prefix, verify_download)
