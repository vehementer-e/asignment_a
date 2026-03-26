from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .models import DQResult, DQStatus
from .registry import DQRuleRegistry


class DataQualityExecutor:
    def __init__(self, registry: Optional[DQRuleRegistry] = None) -> None:
        self.registry = registry or DQRuleRegistry.with_defaults()

    def execute_many(
        self,
        df: Any,
        rules: Iterable[Any],
        runtime_context: Optional[Dict[str, Any]] = None,
        dataset_name: Optional[str] = None,
    ) -> List[DQResult]:
        results: List[DQResult] = []
        ctx = dict(runtime_context or {})
        if dataset_name and 'dataset_name' not in ctx:
            ctx['dataset_name'] = dataset_name

        for rule in rules:
            rule_type = getattr(rule, 'rule_type', None)
            rule_id = getattr(rule, 'rule_id', 'unknown_rule')
            severity = getattr(rule, 'severity', 'warn')
            target_dataset = getattr(rule, 'dataset', None) or dataset_name

            if not rule_type:
                results.append(
                    DQResult(
                        rule_id=rule_id,
                        rule_type='unknown',
                        dataset=target_dataset,
                        severity=severity,
                        status=DQStatus.ERROR,
                        message='Rule is missing rule_type',
                    )
                )
                continue

            if not self.registry.has(rule_type):
                results.append(
                    DQResult(
                        rule_id=rule_id,
                        rule_type=rule_type,
                        dataset=target_dataset,
                        severity=severity,
                        status=DQStatus.ERROR,
                        message=f"Unknown DQ rule type '{rule_type}'",
                    )
                )
                continue

            try:
                result = self.registry.create(rule_type).evaluate(df=df, rule_config=rule, context=ctx)
                if result.dataset is None:
                    result.dataset = target_dataset
                results.append(result)
            except Exception as exc:  # noqa: BLE001
                results.append(
                    DQResult(
                        rule_id=rule_id,
                        rule_type=rule_type,
                        dataset=target_dataset,
                        severity=severity,
                        status=DQStatus.ERROR,
                        message=str(exc),
                        details={'exception_type': type(exc).__name__},
                    )
                )

        return results
