from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import functions as F

from .base import DataQualityRule
from .models import DQResult, DQStatus


class AcceptedValuesRule(DataQualityRule):
    rule_type = 'accepted_values'

    def evaluate(self, df: Any, rule_config: Any, context: Optional[Dict[str, Any]] = None) -> DQResult:
        column = rule_config.params['column']
        values = rule_config.params['values']
        ignore_nulls = rule_config.params.get('ignore_nulls', True)

        condition = ~F.col(column).isin(list(values))
        if ignore_nulls:
            condition = condition & F.col(column).isNotNull()

        invalid_df = df.filter(condition)
        failed_count = invalid_df.count()
        row_count = df.count()
        sample_values = [r[column] for r in invalid_df.select(column).distinct().limit(10).collect()]
        status = DQStatus.PASSED if failed_count == 0 else DQStatus.FAILED
        return DQResult(
            rule_id=rule_config.rule_id,
            rule_type=self.rule_type,
            dataset=getattr(rule_config, 'dataset', None) or (context or {}).get('dataset'),
            column=column,
            severity=rule_config.severity,
            status=status,
            metric_name='invalid_value_count',
            metric_value=float(failed_count),
            threshold=0,
            failed_count=failed_count,
            row_count=row_count,
            message=f"Column '{column}' has {failed_count} invalid values",
            details={'accepted_values': list(values), 'sample_invalid_values': sample_values},
        )
