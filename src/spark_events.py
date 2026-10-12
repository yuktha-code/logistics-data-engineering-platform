"""PySpark ETL: validate/deduplicate events and create city/month Parquet partitions.

The exact-record duplicate policy is intentionally equivalent to src.pipeline:
conflicting duplicate event IDs must already have failed source prevalidation.
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from .mongo_events import EVENT_FIELDS


def transform_events_with_spark(raw_dir: Path, processed_dir: Path,
                                parquet_dir: Path,
                                expected_unique: int,
                                expected_raw: int,
                                master: str = 'local[2]') -> dict[str, int]:
    """Write a clean CSV for the existing SQL loader and partitioned Parquet archive.

    All source tables are prevalidated by caller before running this stage.
    Writes data to a temporary processed directory first, never editing inputs.
    """
    try:
        from pyspark.sql import SparkSession, functions as F, types as T
    except ImportError as exc:
        raise RuntimeError('Missing PySpark: install Python 3.11 and requirements-advanced.txt') from exc

    raw_dir, processed_dir, parquet_dir = map(Path, (raw_dir, processed_dir, parquet_dir))
    processed_dir.parent.mkdir(parents=True, exist_ok=True)
    parquet_dir.parent.mkdir(parents=True, exist_ok=True)
    if processed_dir.resolve() == raw_dir.resolve():
        raise ValueError('processed_dir cannot overwrite original raw source')
    if parquet_dir.resolve() in {processed_dir.resolve(), raw_dir.resolve()}:
        raise ValueError('Parquet destination must differ from raw/processed directories')

    spark = (SparkSession.builder.master(master).appName('logistics-delivery-event-etl')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '8')
             .getOrCreate())
    tmp_processed = Path(tempfile.mkdtemp(prefix='spark_clean_', dir=processed_dir.parent))
    tmp_archive = Path(tempfile.mkdtemp(prefix='spark_archive_', dir=parquet_dir.parent))
    try:
        event_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in EVENT_FIELDS])
        # String schema preserves timestamps/IDs verbatim for SQL loader.
        events = spark.read.option('header', 'true').schema(event_schema).csv(str(raw_dir/'events.csv'))
        observed_raw = events.count()
        if observed_raw != expected_raw:
            raise ValueError(f'Spark raw event row mismatch {observed_raw} != {expected_raw}')
        unique = events.dropDuplicates(list(EVENT_FIELDS))
        count = unique.count()
        if count != expected_unique:
            raise ValueError(f'Spark distinct event count mismatch {count} != {expected_unique}')
        # Defence in depth: reject conflicting duplicate event_id in Spark as well.
        conflict = (unique.groupBy('event_id').count().where(F.col('count') > 1).limit(1).count())
        if conflict:
            raise ValueError('Conflicting duplicate event_id after Spark deduplication')
        order_schema = T.StructType([
            T.StructField('order_id', T.StringType(), False),
            T.StructField('city_id', T.StringType(), False),
        ])
        orders = (spark.read.option('header', 'true').csv(str(raw_dir/'orders.csv'))
                  .select('order_id','city_id'))
        # Stable one-order-per-key semantics are already enforced by prevalidation.
        if unique.join(orders, 'order_id', 'left_anti').limit(1).count():
            raise ValueError('Spark found orphan events without parent orders')
        enriched = (unique.join(orders, 'order_id', 'inner')
                    .withColumn('event_month', F.date_format(
                        F.to_timestamp('event_at', "yyyy-MM-dd'T'HH:mm:ssX"), 'yyyy-MM')))
        if enriched.where(F.col('event_month').isNull()).limit(1).count():
            raise ValueError('Spark failed to parse event timestamps')
        if enriched.count() != count:
            raise ValueError('Spark join multiplied event rows')

        # Event-level analytics archive: real Spark Parquet, not CSV with renamed extension.
        (enriched.repartition(8, 'city_id').write.mode('overwrite')
         .partitionBy('city_id','event_month').parquet(str(tmp_archive)))
        # Make Spark output the actual source for the downstream SQL loader.
        csv_parts = tmp_processed/'spark_csv_parts'
        (unique.select(*EVENT_FIELDS).coalesce(1).write.mode('overwrite')
         .option('header', True).csv(str(csv_parts)))
        parts = list(csv_parts.glob('part-*.csv'))
        if len(parts) != 1:
            raise ValueError('Spark CSV export did not produce exactly one data part')
        shutil.move(str(parts[0]), str(tmp_processed/'events.csv'))
        shutil.rmtree(csv_parts)
        for filename in ('orders.csv','cities.csv','customers.csv','drivers.csv'):
            shutil.copy2(raw_dir/filename, tmp_processed/filename)
        # Verify the archive matches the source BEFORE publishing either destination.
        archived = spark.read.parquet(str(tmp_archive)).count()
        if archived != count:
            raise ValueError(f'Parquet record reconciliation failed {archived} != {count}')
        if processed_dir.exists():
            shutil.rmtree(processed_dir)
        if parquet_dir.exists():
            shutil.rmtree(parquet_dir)
        tmp_processed.rename(processed_dir)
        tmp_archive.rename(parquet_dir)
        return {'input_events':observed_raw,'deduplicated_events':count,
                'removed_exact_duplicates':observed_raw-count,'parquet_events':archived}
    finally:
        spark.stop()
        if tmp_processed.exists():
            shutil.rmtree(tmp_processed)
        if tmp_archive.exists():
            shutil.rmtree(tmp_archive)
