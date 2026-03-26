from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional


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

    def __init__(self, pipeline_id: str, environment: str, runtime_params: Optional[Mapping[str, Any]] = None):
        self.pipeline_id = pipeline_id
        self.environment = environment
        self.runtime_params = dict(runtime_params or {})

    def build(
        self,
        *,
        pipeline_config: Any,
        environment_config: Any,
        rulepacks: Optional[Iterable[Any]] = None,
        run_id: Optional[str] = None,
        trigger_type: str = "manual",
        source_snapshot: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        cfg_payload = {
            "pipeline": pipeline_config,
            "environment": environment_config,
            "rulepacks": list(rulepacks or []),
        }
        config_hash = build_config_hash(cfg_payload)
        effective_run_id = run_id or self.runtime_params.get("run_id") or self._default_run_id(now)
        return {
            "pipeline_id": self.pipeline_id,
            "environment": self.environment,
            "run_id": effective_run_id,
            "trigger_type": trigger_type,
            "business_date": self.runtime_params.get("business_date"),
            "config_hash": config_hash,
            "config_version": getattr(pipeline_config, "version", None),
            "runtime_params": self.runtime_params,
            "source_snapshot": dict(source_snapshot or {}),
            "started_at": now.isoformat(),
        }

    def _default_run_id(self, now: datetime) -> str:
        ts = now.strftime("%Y%m%dT%H%M%SZ")
        return f"{self.pipeline_id}-{ts}"
