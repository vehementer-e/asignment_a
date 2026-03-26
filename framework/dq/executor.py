from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .models import DQExecutionSummary, DQResult, DQStatus
from .registry import DQRuleRegistry, build_default_registry


class DataQualityExecutor:
    def __init__(self, registry: Optional[DQRuleRegistry] = None) -> None:
        self.registry = registry or build_default_registry()

    def execute_rules(
        self,
        df: Any,
        rules: Iterable[Any],
        dataset: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> DQExecutionSummary:
        results: List[DQResult] = []
        ctx = dict(context or {})
        if dataset and 'dataset' not in ctx:
            ctx['dataset'] = dataset

        for rule in rules:
            rule_type = getattr(rule, 'rule_type', None)
            rule_id = getattr(rule, 'rule_id', 'unknown_rule')
            severity = getattr(rule, 'severity', 'warn')
            target_dataset = getattr(rule, 'dataset', None) or dataset

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

        return DQExecutionSummary.from_results(
            results=results,
            pipeline_id=ctx.get('pipeline_id'),
            run_id=ctx.get('run_id'),
            dataset=dataset,
        )
