from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class InMemoryControlLogger:
    run_records: List[Dict[str, Any]] = field(default_factory=list)
    step_records: List[Dict[str, Any]] = field(default_factory=list)

    def log_run_start(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        record = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "status": "RUNNING",
            "payload": dict(payload or {}),
            "started_at": self._utcnow(),
            "updated_at": self._utcnow(),
        }
        self.run_records.append(record)
        return record

    def log_run_end(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        status: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        record = self._find_run(run_id)
        record.update(
            {
                "pipeline_id": pipeline_id,
                "status": status,
                "payload": {**record.get("payload", {}), **dict(payload or {})},
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
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        record = {
            "run_id": run_id,
            "step_id": step_id,
            "status": "RUNNING",
            "payload": dict(payload or {}),
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
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        record = self._find_step(run_id=run_id, step_id=step_id)
        record.update(
            {
                "status": status,
                "payload": {**record.get("payload", {}), **dict(payload or {})},
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
