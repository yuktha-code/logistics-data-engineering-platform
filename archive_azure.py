"""Command-line optional cloud archive stage; never stores secret tokens in output."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from src.azure_archive import upload_to_azure

ROOT = Path(__file__).resolve().parent


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description='Sync verified Parquet partitions to Azure Blob Storage')
    p.add_argument('--parquet-dir', type=Path, default=ROOT/'data'/'parquet'/'delivery_events')
    p.add_argument('--container', default=os.getenv('AZURE_BLOB_CONTAINER', 'logistics-archive'))
    p.add_argument('--prefix', default=os.getenv('AZURE_BLOB_PREFIX', 'logistics/events/v1'))
    p.add_argument('--results-dir', type=Path, default=ROOT/'results')
    p.add_argument('--skip-download-verification', action='store_true')
    args = p.parse_args(argv)
    report = upload_to_azure(args.parquet_dir, args.container,
                             connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
                             account_url=os.getenv('AZURE_STORAGE_ACCOUNT_URL'),
                             prefix=args.prefix,
                             verify_download=not args.skip_download_verification)
    args.results_dir.mkdir(parents=True, exist_ok=True)
    (args.results_dir/'azure_archive_run.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('data_files', 'uploaded','already_present',
                                          'download_verified','bytes', 'manifest_blob')}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
