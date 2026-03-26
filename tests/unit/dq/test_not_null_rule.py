from __future__ import annotations

from types import SimpleNamespace

from framework.dq.models import DQStatus
from framework.dq.not_null import NotNullRule


def test_not_null_rule_returns_failed_when_nulls_present(spark) -> None:
    df = spark.createDataFrame(
        [
            ('c1', 'Alice'),
            (None, 'Broken'),
            ('c3', 'Charlie'),
        ],
        ['client_id', 'name'],
    )

    rule_config = SimpleNamespace(
        rule_id='client_id_not_null',
        params={'column': 'client_id'},
        severity='fail',
    )

    result = NotNullRule().evaluate(df=df, rule_config=rule_config, context={'dataset': 'clients'})

    assert result.status == DQStatus.FAILED
    assert result.failed_count == 1
    assert result.row_count == 3
    assert result.column == 'client_id'
