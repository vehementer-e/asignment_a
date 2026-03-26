from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import functions as F

from .base import DataQualityRule
from .models import DQResult, DQStatus


class NotNullRule(DataQualityRule):
    rule_type = 'not_null'

    def evaluate(self, df: Any, rule_config: Any, context: Optional[Dict[str, Any]] = None) -> DQResult:
        column = rule_config.params['column']
        failed_count = df.filter(F.col(column).isNull()).count()
        row_count = df.count()
        status = DQStatus.PASSED if failed_count == 0 else DQStatus.FAILED
        return DQResult(
            rule_id=rule_config.rule_id,
            rule_type=self.rule_type,
            dataset=getattr(rule_config, 'dataset', None) or (context or {}).get('dataset'),
            column=column,
            severity=rule_config.severity,
            status=status,
            metric_name='null_count',
            metric_value=float(failed_count),
            threshold=0,
            failed_count=failed_count,
            row_count=row_count,
            message=f"Column '{column}' has {failed_count} null rows",
        )
