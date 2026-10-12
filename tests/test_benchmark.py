"""SQL benchmark invariants; tiny size keeps routine CI fast."""
from bench_sql_pipeline import run_once


def test_benchmark_asserts_source_counts_and_idempotent_replay():
    result=run_once(20,seed=42)
    assert result['orders']==20
    assert result['unique_events']<=result['raw_events']
    assert result['sql_load_seconds']>0
    assert result['replay_inserted_orders']==0
    assert result['replay_inserted_events']==0
