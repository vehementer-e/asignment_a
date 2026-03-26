from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark.sql import functions as F

from .base import DataQualityRule
from .models import DQResult, DQStatus


class UniquenessRule(DataQualityRule):
    rule_type = 'uniqueness'

    def evaluate(self, df: Any, rule_config: Any, context: Optional[Dict[str, Any]] = None) -> DQResult:
        columns: List[str] = rule_config.params['columns']
        row_count = df.count()
        duplicate_rows = (
            df.groupBy(*[F.col(c) for c in columns])
            .count()
            .filter(F.col('count') > 1)
        )
        duplicate_group_count = duplicate_rows.count()
        duplicate_row_count = 0
        if duplicate_group_count > 0:
            duplicate_row_count = duplicate_rows.agg(F.sum('count').alias('total')).collect()[0]['total']
        status = DQStatus.PASSED if duplicate_group_count == 0 else DQStatus.FAILED
        return DQResult(
            rule_id=rule_config.rule_id,
            rule_type=self.rule_type,
            dataset=getattr(rule_config, 'dataset', None) or (context or {}).get('dataset'),
            severity=rule_config.severity,
            status=status,
            metric_name='duplicate_groups',
            metric_value=float(duplicate_group_count),
            threshold=0,
            failed_count=int(duplicate_row_count or 0),
            row_count=row_count,
            message=f"Columns {columns} have {duplicate_group_count} duplicate key groups",
            details={
                'columns': columns,
                'duplicate_groups': duplicate_group_count,
                'duplicate_rows': int(duplicate_row_count or 0),
            },
        )
