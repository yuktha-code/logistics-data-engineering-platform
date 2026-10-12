"""MongoDB raw-event mirror with stable event IDs and bounded bulk upserts.

The warehouse remains authoritative for analytical marts; MongoDB is a separately
replayable document store for raw validated event records. No cross-store ACID
transaction is claimed.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

EVENT_FIELDS = ('event_id', 'order_id', 'event_type', 'event_at',
                'ingested_at', 'latitude', 'longitude', 'source')


def chunks(rows: Iterable[dict[str, Any]], size: int = 500) -> Iterator[list[dict[str, Any]]]:
    if size < 1:
        raise ValueError('batch size must be positive')
    batch: list[dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def event_document(event: dict[str, Any]) -> dict[str, Any]:
    """Store raw values exactly (including blank coordinates) to retain provenance."""
    missing = [key for key in EVENT_FIELDS if key not in event]
    if missing:
        raise ValueError(f'event missing fields: {missing}')
    if not event['event_id'] or not event['order_id']:
        raise ValueError('event_id and order_id must be populated')
    return {'_id': event['event_id'], **{key: event[key] for key in EVENT_FIELDS}}


def mirror_events_to_mongo(events: Iterable[dict[str, Any]], mongo_uri: str,
                           database: str = 'logistics_dev',
                           collection: str = 'tracking_events',
                           batch_size: int = 500) -> dict[str, int]:
    """Upsert prevalidated events using _id=event_id; fail closed on Mongo outages.

    This function must be invoked only after read_csv_tables+clean_validate,
    which reject conflicting duplicate keys and malformed event data.
    """
    try:
        from pymongo import MongoClient, ReplaceOne, ASCENDING
    except ImportError as exc:
        raise RuntimeError('Missing PyMongo: pip install -r requirements-advanced.txt') from exc
    if not database or not collection:
        raise ValueError('MongoDB database and collection must be nonempty')
    stats = {'input_events': 0, 'upserted': 0, 'matched': 0, 'modified': 0}
    with MongoClient(mongo_uri, serverSelectionTimeoutMS=10000) as client:
        client.admin.command('ping')
        target = client[database][collection]
        target.create_index([('order_id', ASCENDING), ('event_at', ASCENDING)],
                            name='order_event_time')
        target.create_index([('source', ASCENDING)], name='source_lookup')
        for batch in chunks(events, batch_size):
            docs = [event_document(row) for row in batch]
            # Replacements update corrections without introducing duplicate IDs.
            operations = [ReplaceOne({'_id': doc['_id']}, doc, upsert=True) for doc in docs]
            result = target.bulk_write(operations, ordered=True)
            stats['input_events'] += len(docs)
            stats['upserted'] += result.upserted_count
            stats['matched'] += result.matched_count
            stats['modified'] += result.modified_count
    return stats
