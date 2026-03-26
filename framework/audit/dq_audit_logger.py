from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class InMemoryDQAuditLogger:
    records: List[Dict[str, Any]] = field(default_factory=list)

    def log_result(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        dataset: Optional[str],
        rule_result: Any,
        stage: Optional[str] = None,
    ) -> Dict[str, Any]:
        if hasattr(rule_result, "model_dump"):
            payload = rule_result.model_dump(mode="json")
        elif isinstance(rule_result, dict):
            payload = dict(rule_result)
        else:
            payload = {"value": str(rule_result)}

        record = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "dataset": dataset,
            "stage": stage,
            **payload,
            "logged_at": self._utcnow(),
        }
        self.records.append(record)
        return record

    def log_many(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        dataset: Optional[str],
        results: Iterable[Any],
        stage: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return [
            self.log_result(
                run_id=run_id,
                pipeline_id=pipeline_id,
                dataset=dataset,
                rule_result=result,
                stage=stage,
            )
            for result in results
        ]

    @staticmethod
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()


class DQAuditLogger(InMemoryDQAuditLogger):
    """Baseline audit logger.

    Compatible with the prototype executor and easy to replace with a Delta-backed implementation.
    """

    pass
