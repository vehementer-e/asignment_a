from __future__ import annotations

from types import SimpleNamespace

from framework.transformations.base import TransformationContext
from framework.transformations.deduplicate import DeduplicateTransformation


def test_deduplicate_keeps_latest_record_per_key(spark) -> None:
    df = spark.createDataFrame(
        [
            ('c1', '2026-03-20', 'old'),
            ('c1', '2026-03-22', 'new'),
            ('c2', '2026-03-21', 'only'),
        ],
        ['client_id', 'ingestion_ts', 'payload'],
    )

    step_config = SimpleNamespace(
        step_id='dedup_clients',
        params={
            'keys': ['client_id'],
            'order_by': [{'column': 'ingestion_ts', 'direction': 'desc'}],
            'keep': 'first',
        },
    )
    context = TransformationContext(spark=spark, datasets={})

    result = DeduplicateTransformation().apply(df, step_config, context)
    rows = {(r['client_id'], r['payload']) for r in result.collect()}

    assert rows == {('c1', 'new'), ('c2', 'only')}
