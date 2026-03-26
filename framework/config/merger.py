from __future__ import annotations

import copy
from typing import Any, Mapping

from .models import EnvironmentConfig, PipelineConfig, RulePackConfig, RuntimeInvocation
from .validator import ConfigValidationError


def merge_runtime_values(
    pipeline: PipelineConfig,
    environment: EnvironmentConfig,
    runtime_invocation: RuntimeInvocation,
) -> dict[str, Any]:
    """
    Build the effective runtime dictionary in precedence order:
      pipeline defaults < environment runtime_defaults < invocation runtime
    """
    runtime: dict[str, Any] = {}

    for param in pipeline.runtime_parameters:
        if param.default is not None:
            runtime[param.name] = copy.deepcopy(param.default)

    runtime.update(copy.deepcopy(environment.runtime_defaults))
    runtime.update(copy.deepcopy(runtime_invocation.runtime))

    missing_required = [
        param.name
        for param in pipeline.runtime_parameters
        if param.required and runtime.get(param.name) is None
    ]
    if missing_required:
        raise ConfigValidationError(
            "Missing required runtime parameter(s) after merge: " + ", ".join(sorted(missing_required))
        )

    return runtime


def merge_rulepacks(
    pipeline: PipelineConfig,
    rulepacks: Mapping[str, RulePackConfig],
) -> list[dict[str, Any]]:
    """Return the effective ordered list of DQ rules: included rulepacks first, inline rules after."""
    merged_rules: list[dict[str, Any]] = []
    seen_rule_ids: set[str] = set()

    for rulepack_id in pipeline.dq.include_rulepacks:
        if rulepack_id not in rulepacks:
            raise ConfigValidationError(f"Referenced rulepack '{rulepack_id}' was not loaded")
        for rule in rulepacks[rulepack_id].rules:
            if rule.rule_id in seen_rule_ids:
                raise ConfigValidationError(
                    f"Duplicate dq rule_id '{rule.rule_id}' while merging rulepacks for pipeline '{pipeline.pipeline_id}'"
                )
            merged_rules.append(rule.model_dump(mode="python"))
            seen_rule_ids.add(rule.rule_id)

    for rule in pipeline.dq.rules:
        if rule.rule_id in seen_rule_ids:
            raise ConfigValidationError(
                f"Inline dq rule_id '{rule.rule_id}' duplicates a rulepack rule in pipeline '{pipeline.pipeline_id}'"
            )
        merged_rules.append(rule.model_dump(mode="python"))
        seen_rule_ids.add(rule.rule_id)

    return merged_rules


def build_execution_bundle(
    pipeline: PipelineConfig,
    environment: EnvironmentConfig,
    runtime_invocation: RuntimeInvocation,
    rulepacks: Mapping[str, RulePackConfig],
) -> dict[str, Any]:
    """
    Build one execution-friendly dictionary that executors can consume directly.
    This keeps typed models at the edges, but hands a plain dict to runtime code.
    """
    effective_runtime = merge_runtime_values(pipeline, environment, runtime_invocation)
    effective_rules = merge_rulepacks(pipeline, rulepacks)

    pipeline_dict = pipeline.model_dump(mode="python")
    pipeline_dict["dq"]["effective_rules"] = effective_rules

    return {
        "pipeline": pipeline_dict,
        "environment": environment.model_dump(mode="python"),
        "runtime": effective_runtime,
        "metadata": {
            "pipeline_id": pipeline.pipeline_id,
            "environment": environment.environment,
            "rulepack_ids": list(pipeline.dq.include_rulepacks),
            "runtime_keys": sorted(effective_runtime.keys()),
        },
    }


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Small utility for recursively merging nested dictionaries."""
    result = copy.deepcopy(dict(base))
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, Mapping):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result
