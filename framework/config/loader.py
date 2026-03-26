from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from .models import (
    EnvironmentConfig,
    LoadedConfigBundle,
    PipelineConfig,
    RulePackConfig,
    RuntimeInvocation,
)
from .validator import (
    ConfigValidationError,
    discover_rulepack_ids,
    load_environment_config,
    load_pipeline_config,
    load_rulepack_config,
    load_runtime_invocation,
    validate_pipeline_semantics,
    validate_runtime_against_pipeline,
)

_PLACEHOLDER_PATTERN = re.compile(r"\$\{([^{}]+)\}")

DEFAULT_ALLOWED_TRANSFORMATIONS = (
    "select_columns",
    "rename_columns",
    "cast_columns",
    "filter_rows",
    "join",
    "derive_columns",
    "deduplicate",
    "sql_transform",
)


class ConfigLoader:
    """Load, resolve, and validate ETL framework configs from disk."""

    def __init__(
        self,
        config_root: str | Path,
        allowed_transformations: Iterable[str] | None = None,
    ) -> None:
        self.config_root = Path(config_root)
        self.allowed_transformations = tuple(allowed_transformations or DEFAULT_ALLOWED_TRANSFORMATIONS)

    def load_environment(self, environment: str) -> EnvironmentConfig:
        path = self.config_root / "environments" / f"{environment}.yaml"
        if not path.exists():
            raise ConfigValidationError(f"Environment config not found: '{path}'")
        return load_environment_config(path)

    def load_pipeline(self, pipeline_id: str) -> PipelineConfig:
        path = self.config_root / "pipelines" / f"{pipeline_id}.yaml"
        if not path.exists():
            raise ConfigValidationError(f"Pipeline config not found: '{path}'")
        return load_pipeline_config(path)

    def load_runtime_file(self, runtime_file: str | Path) -> RuntimeInvocation:
        return load_runtime_invocation(runtime_file)

    def load_rulepacks(self) -> dict[str, RulePackConfig]:
        rulepack_dir = self.config_root / "dq_rulepacks"
        if not rulepack_dir.exists():
            return {}
        result: dict[str, RulePackConfig] = {}
        for path in sorted(rulepack_dir.glob("*.yaml")):
            rulepack = load_rulepack_config(path)
            result[rulepack.rulepack_id] = rulepack
        return result

    def load_bundle(
        self,
        pipeline_id: str,
        environment: str,
        runtime_file: str | Path | None = None,
        runtime_overrides: Mapping[str, Any] | None = None,
    ) -> LoadedConfigBundle:
        """
        Load a complete config bundle and return a typed LoadedConfigBundle.
        """
        env_cfg = self.load_environment(environment)
        pipeline_cfg = self.load_pipeline(pipeline_id)
        rulepacks = self.load_rulepacks()

        validate_pipeline_semantics(
            pipeline_cfg,
            available_rulepacks=discover_rulepack_ids(self.config_root / "dq_rulepacks"),
            allowed_transformations=self.allowed_transformations,
        )

        runtime_invocation = self._build_runtime_invocation(
            pipeline_id=pipeline_id,
            environment=environment,
            runtime_file=runtime_file,
            runtime_overrides=runtime_overrides,
        )
        validate_runtime_against_pipeline(runtime_invocation, pipeline_cfg, env_cfg)

        resolved_pipeline = resolve_pipeline_placeholders(
            pipeline_cfg.model_dump(mode="python"),
            environment=env_cfg.model_dump(mode="python"),
            runtime=runtime_invocation.runtime,
        )

        return LoadedConfigBundle(
            pipeline_config=pipeline_cfg,
            environment_config=env_cfg,
            runtime_invocation=runtime_invocation,
            rulepacks=rulepacks,
            runtime_overrides=dict(runtime_overrides or {}),
            resolved_pipeline_config=resolved_pipeline,
        )

    def _build_runtime_invocation(
        self,
        pipeline_id: str,
        environment: str,
        runtime_file: str | Path | None = None,
        runtime_overrides: Mapping[str, Any] | None = None,
    ) -> RuntimeInvocation:
        if runtime_file:
            runtime_invocation = self.load_runtime_file(runtime_file)
            runtime_payload = dict(runtime_invocation.runtime)
        else:
            runtime_payload = {}

        if runtime_overrides:
            runtime_payload.update(dict(runtime_overrides))

        return RuntimeInvocation(
            pipeline_id=pipeline_id,
            environment=environment,
            runtime=runtime_payload,
        )


def resolve_pipeline_placeholders(
    pipeline_data: Mapping[str, Any],
    *,
    environment: Mapping[str, Any],
    runtime: Mapping[str, Any],
    strict: bool = True,
) -> dict[str, Any]:
    """
    Resolve ${...} placeholders inside a pipeline dict.

    Supported roots:
      - databricks.*   -> environment["databricks"]
      - storage.*      -> environment["storage"]
      - control_tables.* -> environment["control_tables"]
      - runtime.*      -> runtime invocation dictionary
      - environment.*  -> full environment dictionary

    Example:
      ${storage.silver_root}
      ${databricks.default_catalog}
      {{ runtime.business_date }} is intentionally NOT resolved here; it is left for Jinja/sql layer.
    """
    context = {
        "databricks": environment.get("databricks", {}),
        "storage": environment.get("storage", {}),
        "control_tables": environment.get("control_tables", {}),
        "runtime": dict(runtime),
        "environment": dict(environment),
    }
    resolved = _resolve_value(copy.deepcopy(dict(pipeline_data)), context, strict=strict)
    return resolved


def load_yaml_file(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigValidationError(f"Config at '{path}' must deserialize to a mapping/object.")
    return data


def _resolve_value(value: Any, context: Mapping[str, Any], *, strict: bool) -> Any:
    if isinstance(value, dict):
        return {key: _resolve_value(inner, context, strict=strict) for key, inner in value.items()}
    if isinstance(value, list):
        return [_resolve_value(item, context, strict=strict) for item in value]
    if isinstance(value, str):
        return _resolve_string(value, context, strict=strict)
    return value


def _resolve_string(template: str, context: Mapping[str, Any], *, strict: bool) -> Any:
    matches = list(_PLACEHOLDER_PATTERN.finditer(template))
    if not matches:
        return template

    if len(matches) == 1 and matches[0].span() == (0, len(template)):
        value = _lookup_path(matches[0].group(1), context, strict=strict)
        return value

    def replacement(match: re.Match[str]) -> str:
        value = _lookup_path(match.group(1), context, strict=strict)
        return "" if value is None else str(value)

    return _PLACEHOLDER_PATTERN.sub(replacement, template)


def _lookup_path(path_expr: str, context: Mapping[str, Any], *, strict: bool) -> Any:
    parts = [part.strip() for part in path_expr.split(".") if part.strip()]
    current: Any = context
    traversed: list[str] = []

    for part in parts:
        traversed.append(part)
        if isinstance(current, Mapping) and part in current:
            current = current[part]
            continue
        if strict:
            raise ConfigValidationError(
                f"Unable to resolve placeholder '${{{path_expr}}}' at '{'.'.join(traversed)}'"
            )
        return f"${{{path_expr}}}"

    return current
