"""Thin helpers for Airflow DAG generation/runtime metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DagRenderRequest:
    pipeline_id: str
    environment: str
    schedule: str | None = None
    tags: list[str] = field(default_factory=list)
    runtime_defaults: dict[str, Any] = field(default_factory=dict)


def build_default_tags(pipeline_id: str, environment: str) -> list[str]:
    return [pipeline_id, environment, "config-driven", "databricks"]
