from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class InMemoryControlLogger:
    run_records: List[Dict[str, Any]] = field(default_factory=list)
    step_records: List[Dict[str, Any]] = field(default_factory=list)

    def log_run_start(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        record = {
            **metadata,
            "status": "RUNNING",
            "updated_at": self._utcnow(),
        }
        self.run_records.append(record)
        return record

    def log_run_end(
        self,
        *,
        run_id: str,
        status: str,
        summary: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        record = self._find_run(run_id)
        record.update(
            {
                "status": status,
                "summary": dict(summary or {}),
                "error": error,
                "ended_at": self._utcnow(),
                "updated_at": self._utcnow(),
            }
        )
        return record

    def log_step_start(
        self,
        *,
        run_id: str,
        step_id: str,
        step_type: str,
        stage: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        record = {
            "run_id": run_id,
            "step_id": step_id,
            "step_type": step_type,
            "stage": stage,
            "status": "RUNNING",
            "details": dict(details or {}),
            "started_at": self._utcnow(),
            "updated_at": self._utcnow(),
        }
        self.step_records.append(record)
        return record

    def log_step_end(
        self,
        *,
        run_id: str,
        step_id: str,
        status: str,
        input_count: Optional[int] = None,
        output_count: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        record = self._find_step(run_id=run_id, step_id=step_id)
        record.update(
            {
                "status": status,
                "input_count": input_count,
                "output_count": output_count,
                "details": {**record.get("details", {}), **dict(details or {})},
                "error": error,
                "ended_at": self._utcnow(),
                "updated_at": self._utcnow(),
            }
        )
        return record

    def _find_run(self, run_id: str) -> Dict[str, Any]:
        for rec in reversed(self.run_records):
            if rec.get("run_id") == run_id:
                return rec
        raise KeyError(f"Unknown run_id: {run_id}")

    def _find_step(self, *, run_id: str, step_id: str) -> Dict[str, Any]:
        for rec in reversed(self.step_records):
            if rec.get("run_id") == run_id and rec.get("step_id") == step_id:
                return rec
        raise KeyError(f"Unknown step record for run_id={run_id}, step_id={step_id}")

    @staticmethod
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()


class ControlLogger(InMemoryControlLogger):
    """Alias for the baseline implementation.

    In the prototype, this logger keeps records in memory.
    In a Databricks deployment, the same interface can be backed by Delta tables.
    """

    pass
