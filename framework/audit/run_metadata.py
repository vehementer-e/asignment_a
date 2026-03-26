from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional


def _json_default(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, set):
        return sorted(value)
    return str(value)


def build_config_hash(payload: Any) -> str:
    normalized = json.dumps(payload, sort_keys=True, default=_json_default, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class RunMetadataBuilder:
    """Builds deterministic run metadata used for audit and replay."""

    def __init__(self, pipeline_id: str, environment: str):
        self.pipeline_id = pipeline_id
        self.environment = environment

    def build(
        self,
        *,
        pipeline_config: Any,
        environment_config: Any,
        runtime_context: Optional[Mapping[str, Any]] = None,
        trigger_type: str = "manual",
        source_snapshot: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        runtime_context = dict(runtime_context or {})
        rulepacks = list(runtime_context.get("effective_dq_rules") or [])
        cfg_payload = {
            "pipeline": pipeline_config,
            "environment": environment_config,
            "rulepacks": rulepacks,
        }
        config_hash = build_config_hash(cfg_payload)
        effective_run_id = runtime_context.get("run_id") or self._default_run_id(now)
        return {
            "pipeline_id": self.pipeline_id,
            "environment": self.environment,
            "run_id": effective_run_id,
            "trigger_type": trigger_type,
            "business_date": runtime_context.get("business_date"),
            "config_hash": config_hash,
            "config_version": getattr(pipeline_config, "version", None),
            "runtime_params": runtime_context,
            "source_snapshot": dict(source_snapshot or {}),
            "started_at": now.isoformat(),
        }

    def _default_run_id(self, now: datetime) -> str:
        ts = now.strftime("%Y%m%dT%H%M%SZ")
        return f"{self.pipeline_id}-{ts}"
