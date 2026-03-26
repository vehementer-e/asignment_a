from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import functions as F

from .base import DataQualityRule
from .models import DQResult, DQStatus


class NumericRangeRule(DataQualityRule):
    rule_type = 'numeric_range'

    def evaluate(self, df: Any, rule_config: Any, context: Optional[Dict[str, Any]] = None) -> DQResult:
        column = rule_config.params['column']
        min_value = rule_config.params.get('min_value')
        max_value = rule_config.params.get('max_value')
        ignore_nulls = rule_config.params.get('ignore_nulls', True)

        condition = None
        if min_value is not None:
            condition = F.col(column) < F.lit(min_value)
        if max_value is not None:
            max_condition = F.col(column) > F.lit(max_value)
            condition = max_condition if condition is None else (condition | max_condition)
        if condition is None:
            raise ValueError(f"Rule '{rule_config.rule_id}' must define min_value and/or max_value")
        if ignore_nulls:
            condition = condition & F.col(column).isNotNull()

        invalid_df = df.filter(condition)
        failed_count = invalid_df.count()
        row_count = df.count()
        stats = df.agg(F.min(column).alias('observed_min'), F.max(column).alias('observed_max')).collect()[0]
        status = DQStatus.PASSED if failed_count == 0 else DQStatus.FAILED
        return DQResult(
            rule_id=rule_config.rule_id,
            rule_type=self.rule_type,
            dataset=getattr(rule_config, 'dataset', None) or (context or {}).get('dataset'),
            column=column,
            severity=rule_config.severity,
            status=status,
            metric_name='out_of_range_count',
            metric_value=float(failed_count),
            threshold={'min_value': min_value, 'max_value': max_value},
            failed_count=failed_count,
            row_count=row_count,
            message=f"Column '{column}' has {failed_count} out-of-range values",
            details={'observed_min': stats['observed_min'], 'observed_max': stats['observed_max']},
        )
