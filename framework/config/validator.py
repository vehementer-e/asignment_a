from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, List, Sequence

import yaml
from pydantic import ValidationError

from .models import (
    DQRuleConfig,
    EnvironmentConfig,
    PipelineConfig,
    RulePackConfig,
    RuntimeInvocation,
    TransformationStepConfig,
    ReadStepConfig,
)


class ConfigValidationError(ValueError):
    """Raised when a config is structurally or semantically invalid."""


def _load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigValidationError(f"Config at '{path}' must deserialize to a mapping/object.")
    return data


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    try:
        return PipelineConfig.model_validate(_load_yaml(path))
    except ValidationError as exc:
        raise ConfigValidationError(f"Pipeline config '{path}' is invalid:\n{exc}") from exc


def load_environment_config(path: str | Path) -> EnvironmentConfig:
    try:
        return EnvironmentConfig.model_validate(_load_yaml(path))
    except ValidationError as exc:
        raise ConfigValidationError(f"Environment config '{path}' is invalid:\n{exc}") from exc


def load_rulepack_config(path: str | Path) -> RulePackConfig:
    try:
        return RulePackConfig.model_validate(_load_yaml(path))
    except ValidationError as exc:
        raise ConfigValidationError(f"Rulepack config '{path}' is invalid:\n{exc}") from exc


def load_runtime_invocation(path: str | Path) -> RuntimeInvocation:
    try:
        return RuntimeInvocation.model_validate(_load_yaml(path))
    except ValidationError as exc:
        raise ConfigValidationError(f"Runtime params '{path}' are invalid:\n{exc}") from exc


def validate_pipeline_semantics(
    pipeline: PipelineConfig,
    available_rulepacks: Sequence[str] | None = None,
    allowed_transformations: Sequence[str] | None = None,
) -> None:
    """
    Run semantic checks that are hard or awkward to encode in static schema:
    - unique ids
    - valid source/target references
    - valid dataframe references between steps
    - dq/write references point to produced dataframes
    - known rulepacks / transformations
    """
    errors: List[str] = []

    source_ids = [s.source_id for s in pipeline.sources]
    target_ids = [t.target_id for t in pipeline.targets]
    step_ids = [s.step_id for s in pipeline.steps]

    _collect_duplicates("source_id", source_ids, errors)
    _collect_duplicates("target_id", target_ids, errors)
    _collect_duplicates("step_id", step_ids, errors)

    runtime_param_names = [p.name for p in pipeline.runtime_parameters]
    _collect_duplicates("runtime parameter", runtime_param_names, errors)

    available_rulepacks = set(available_rulepacks or [])
    allowed_transformations = set(allowed_transformations or [])

    produced_dfs: set[str] = set()

    for step in pipeline.steps:
        if isinstance(step, ReadStepConfig):
            if step.source_id not in source_ids:
                errors.append(
                    f"read step '{step.step_id}' references unknown source_id '{step.source_id}'"
                )
            if step.output_df in produced_dfs:
                errors.append(
                    f"step '{step.step_id}' produces duplicate dataframe name '{step.output_df}'"
                )
            produced_dfs.add(step.output_df)
            continue

        if isinstance(step, TransformationStepConfig):
            if step.input_df not in produced_dfs:
                errors.append(
                    f"transformation step '{step.step_id}' references unknown input_df '{step.input_df}'"
                )

            if step.right_df and step.right_df not in produced_dfs:
                errors.append(
                    f"transformation step '{step.step_id}' references unknown right_df '{step.right_df}'"
                )

            if allowed_transformations and step.transformation not in allowed_transformations:
                errors.append(
                    f"transformation step '{step.step_id}' uses unsupported transformation "
                    f"'{step.transformation}'"
                )

            if step.output_df in produced_dfs:
                errors.append(
                    f"step '{step.step_id}' produces duplicate dataframe name '{step.output_df}'"
                )
            produced_dfs.add(step.output_df)

            _validate_transformation_contract(step, errors)

    if pipeline.dq.apply_to not in produced_dfs:
        errors.append(f"dq.apply_to references unknown dataframe '{pipeline.dq.apply_to}'")

    if pipeline.write.input_df not in produced_dfs:
        errors.append(f"write.input_df references unknown dataframe '{pipeline.write.input_df}'")

    if pipeline.write.target_id not in target_ids:
        errors.append(f"write.target_id references unknown target_id '{pipeline.write.target_id}'")

    for pack in pipeline.dq.include_rulepacks:
        if available_rulepacks and pack not in available_rulepacks:
            errors.append(f"pipeline references unknown dq rulepack '{pack}'")

    rule_ids = [rule.rule_id for rule in pipeline.dq.rules]
    _collect_duplicates("inline dq rule_id", rule_ids, errors)

    for rule in pipeline.dq.rules:
        _validate_rule_params(rule, errors)

    if errors:
        raise ConfigValidationError("Pipeline semantic validation failed:\n- " + "\n- ".join(errors))


def validate_runtime_against_pipeline(
    runtime_invocation: RuntimeInvocation,
    pipeline: PipelineConfig,
    environment: EnvironmentConfig | None = None,
) -> None:
    errors: List[str] = []

    if runtime_invocation.pipeline_id != pipeline.pipeline_id:
        errors.append(
            f"runtime pipeline_id '{runtime_invocation.pipeline_id}' does not match "
            f"pipeline config '{pipeline.pipeline_id}'"
        )

    if environment and runtime_invocation.environment != environment.environment:
        errors.append(
            f"runtime environment '{runtime_invocation.environment}' does not match "
            f"environment config '{environment.environment}'"
        )

    runtime_values = runtime_invocation.runtime
    for param in pipeline.runtime_parameters:
        if param.required and param.name not in runtime_values and param.default is None:
            errors.append(f"missing required runtime parameter '{param.name}'")

    unexpected = set(runtime_values.keys()) - {p.name for p in pipeline.runtime_parameters}
    if unexpected:
        errors.append(f"unexpected runtime parameter(s): {sorted(unexpected)}")

    if errors:
        raise ConfigValidationError("Runtime validation failed:\n- " + "\n- ".join(errors))


def discover_rulepack_ids(rulepack_dir: str | Path) -> list[str]:
    ids: list[str] = []
    for path in sorted(Path(rulepack_dir).glob("*.yaml")):
        ids.append(load_rulepack_config(path).rulepack_id)
    return ids


def _collect_duplicates(label: str, items: Iterable[str], errors: list[str]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for item in items:
        if item in seen:
            duplicates.add(item)
        seen.add(item)
    for duplicate in sorted(duplicates):
        errors.append(f"duplicate {label} '{duplicate}'")


def _validate_transformation_contract(step: TransformationStepConfig, errors: list[str]) -> None:
    params = step.params or {}

    if step.transformation == "rename_columns":
        if not isinstance(params.get("mappings"), dict) or not params["mappings"]:
            errors.append(f"step '{step.step_id}' (rename_columns) requires non-empty params.mappings")

    elif step.transformation == "cast_columns":
        if not isinstance(params.get("columns"), dict) or not params["columns"]:
            errors.append(f"step '{step.step_id}' (cast_columns) requires non-empty params.columns")

    elif step.transformation == "derive_columns":
        if not isinstance(params.get("expressions"), dict) or not params["expressions"]:
            errors.append(f"step '{step.step_id}' (derive_columns) requires non-empty params.expressions")

    elif step.transformation == "deduplicate":
        if not isinstance(params.get("key_columns"), list) or not params["key_columns"]:
            errors.append(f"step '{step.step_id}' (deduplicate) requires non-empty params.key_columns")
        order_by = params.get("order_by", [])
        if order_by and not isinstance(order_by, list):
            errors.append(f"step '{step.step_id}' (deduplicate) params.order_by must be a list")

    elif step.transformation == "select_columns":
        if not isinstance(params.get("columns"), list) or not params["columns"]:
            errors.append(f"step '{step.step_id}' (select_columns) requires non-empty params.columns")

    elif step.transformation == "join":
        if not step.right_df:
            errors.append(f"step '{step.step_id}' (join) requires right_df")
        join_condition = params.get("join_condition")
        if not isinstance(join_condition, list) or not join_condition:
            errors.append(f"step '{step.step_id}' (join) requires non-empty params.join_condition")

    elif step.transformation == "filter_rows":
        if not params.get("condition"):
            errors.append(f"step '{step.step_id}' (filter_rows) requires params.condition")

    elif step.transformation == "sql_transform":
        if not (params.get("sql") or params.get("template")):
            errors.append(f"step '{step.step_id}' (sql_transform) requires params.sql or params.template")


def _validate_rule_params(rule: DQRuleConfig, errors: list[str]) -> None:
    params = rule.params or {}

    if rule.rule_type == "not_null":
        if not isinstance(params.get("columns"), list) or not params["columns"]:
            errors.append(f"rule '{rule.rule_id}' (not_null) requires non-empty params.columns")

    elif rule.rule_type == "uniqueness":
        if not isinstance(params.get("columns"), list) or not params["columns"]:
            errors.append(f"rule '{rule.rule_id}' (uniqueness) requires non-empty params.columns")

    elif rule.rule_type == "accepted_values":
        if not params.get("column"):
            errors.append(f"rule '{rule.rule_id}' (accepted_values) requires params.column")
        if not isinstance(params.get("allowed_values"), list) or not params["allowed_values"]:
            errors.append(f"rule '{rule.rule_id}' (accepted_values) requires non-empty params.allowed_values")

    elif rule.rule_type == "numeric_range":
        if not params.get("column"):
            errors.append(f"rule '{rule.rule_id}' (numeric_range) requires params.column")
        if "min_value" not in params and "max_value" not in params:
            errors.append(
                f"rule '{rule.rule_id}' (numeric_range) requires params.min_value or params.max_value"
            )

    elif rule.rule_type == "freshness":
        if not params.get("column"):
            errors.append(f"rule '{rule.rule_id}' (freshness) requires params.column")
        if "max_age_days" not in params:
            errors.append(f"rule '{rule.rule_id}' (freshness) requires params.max_age_days")

    elif rule.rule_type == "referential_integrity":
        required = {"column", "reference_df", "reference_column"}
        missing = sorted(required - set(params.keys()))
        if missing:
            errors.append(
                f"rule '{rule.rule_id}' (referential_integrity) requires params {missing}"
            )

    elif rule.rule_type == "row_count_min":
        if "min_rows" not in params:
            errors.append(f"rule '{rule.rule_id}' (row_count_min) requires params.min_rows")
