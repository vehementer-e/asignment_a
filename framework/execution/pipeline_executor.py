from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

logger = logging.getLogger(__name__)


class PipelineExecutionError(RuntimeError):
    """Raised when pipeline execution fails."""


@dataclass(slots=True)
class ExecutionContext:
    pipeline_id: str
    environment: str
    runtime: dict[str, Any]
    config_hash: str
    started_at: str
    dry_run: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StepResult:
    step_id: str
    status: str
    row_count: Optional[int] = None
    input_df: Optional[str] = None
    output_df: Optional[str] = None
    details: dict[str, Any] = field(default_factory=dict)


class PipelineExecutor:
    """
    Small execution orchestrator that consumes the bundle returned by
    framework.config.merger.build_execution_bundle().

    Design goals:
      - usable today for dry-run / validation-first execution
      - easy to extend with Spark readers, writers, transformations, and DQ engines
      - no hard dependency on pyspark for local config validation/demo runs
    """

    def __init__(
        self,
        *,
        spark: Any | None = None,
        readers: Mapping[str, Callable[..., Any]] | None = None,
        transformations: Mapping[str, Callable[..., Any]] | None = None,
        writer: Callable[..., Any] | None = None,
        dq_executor: Callable[..., Any] | None = None,
        logger_: logging.Logger | None = None,
    ) -> None:
        self.spark = spark
        self.readers = dict(readers or {})
        self.transformations = dict(transformations or {})
        self.writer = writer
        self.dq_executor = dq_executor
        self.logger = logger_ or logger

    def execute(self, bundle: Mapping[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
        pipeline = dict(bundle["pipeline"])
        environment = dict(bundle["environment"])
        runtime = dict(bundle["runtime"])
        metadata = dict(bundle.get("metadata", {}))

        context = ExecutionContext(
            pipeline_id=pipeline["pipeline_id"],
            environment=environment["environment"],
            runtime=runtime,
            config_hash=_hash_bundle(bundle),
            started_at=_utc_now_iso(),
            dry_run=dry_run,
            metadata=metadata,
        )

        self.logger.info(
            "Starting pipeline '%s' in environment '%s' (dry_run=%s)",
            context.pipeline_id,
            context.environment,
            context.dry_run,
        )

        dataframe_store: dict[str, Any] = {}
        step_results: list[StepResult] = []

        for step in pipeline.get("steps", []):
            result = self._execute_step(step, pipeline, environment, context, dataframe_store)
            step_results.append(result)

        dq_result = self._execute_dq(pipeline, context, dataframe_store)
        write_result = self._execute_write(pipeline, environment, context, dataframe_store)

        finished_at = _utc_now_iso()
        final_status = self._derive_pipeline_status(step_results, dq_result, write_result)

        summary = {
            "pipeline_id": context.pipeline_id,
            "environment": context.environment,
            "status": final_status,
            "dry_run": context.dry_run,
            "config_hash": context.config_hash,
            "started_at": context.started_at,
            "finished_at": finished_at,
            "runtime": runtime,
            "steps": [result.__dict__ for result in step_results],
            "dq": dq_result,
            "write": write_result,
            "materialized_dataframes": sorted(dataframe_store.keys()),
        }
        self.logger.info(
            "Pipeline '%s' finished with status=%s",
            context.pipeline_id,
            final_status,
        )
        return summary

    def _execute_step(
        self,
        step: Mapping[str, Any],
        pipeline: Mapping[str, Any],
        environment: Mapping[str, Any],
        context: ExecutionContext,
        dataframe_store: dict[str, Any],
    ) -> StepResult:
        step_id = step["step_id"]
        step_type = step["step_type"]
        self.logger.info("Executing step '%s' (%s)", step_id, step_type)

        if step_type == "read":
            output_df = step["output_df"]
            source = _get_source_by_id(pipeline, step["source_id"])
            if context.dry_run:
                dataframe_store[output_df] = {
                    "kind": "dry_run_dataframe",
                    "source_id": source["source_id"],
                    "source_type": source["source_type"],
                }
                return StepResult(
                    step_id=step_id,
                    status="DRY_RUN",
                    output_df=output_df,
                    details={"source_id": source["source_id"], "source_type": source["source_type"]},
                )

            reader = self.readers.get(source["source_type"])
            if reader is None:
                raise PipelineExecutionError(
                    f"No reader registered for source_type='{source['source_type']}' in step '{step_id}'"
                )
            df = reader(source=source, spark=self.spark, environment=environment, context=context)
            dataframe_store[output_df] = df
            return StepResult(
                step_id=step_id,
                status="SUCCESS",
                output_df=output_df,
                row_count=_safe_row_count(df),
                details={"source_id": source["source_id"]},
            )

        if step_type == "transformation":
            transformation_name = step["transformation"]
            input_df_name = step["input_df"]
            output_df_name = step["output_df"]
            input_df = dataframe_store[input_df_name]
            right_df = dataframe_store.get(step.get("right_df")) if step.get("right_df") else None

            if context.dry_run:
                dataframe_store[output_df_name] = {
                    "kind": "dry_run_dataframe",
                    "derived_from": input_df_name,
                    "transformation": transformation_name,
                }
                return StepResult(
                    step_id=step_id,
                    status="DRY_RUN",
                    input_df=input_df_name,
                    output_df=output_df_name,
                    details={
                        "transformation": transformation_name,
                        "params": step.get("params", {}),
                        "has_right_df": bool(step.get("right_df")),
                    },
                )

            transform_fn = self.transformations.get(transformation_name)
            if transform_fn is None:
                raise PipelineExecutionError(
                    f"No transformation registered for '{transformation_name}' in step '{step_id}'"
                )
            result_df = transform_fn(
                df=input_df,
                right_df=right_df,
                params=step.get("params", {}),
                spark=self.spark,
                context=context,
            )
            dataframe_store[output_df_name] = result_df
            return StepResult(
                step_id=step_id,
                status="SUCCESS",
                input_df=input_df_name,
                output_df=output_df_name,
                row_count=_safe_row_count(result_df),
                details={"transformation": transformation_name},
            )

        raise PipelineExecutionError(f"Unsupported step_type='{step_type}' in step '{step_id}'")

    def _execute_dq(
        self,
        pipeline: Mapping[str, Any],
        context: ExecutionContext,
        dataframe_store: Mapping[str, Any],
    ) -> dict[str, Any]:
        dq_cfg = dict(pipeline.get("dq", {}))
        apply_to = dq_cfg.get("apply_to")
        rules = list(dq_cfg.get("effective_rules", dq_cfg.get("rules", [])))

        if not apply_to:
            return {"status": "SKIPPED", "reason": "No dq.apply_to configured", "results": []}

        if apply_to not in dataframe_store:
            raise PipelineExecutionError(
                f"dq.apply_to='{apply_to}' was not materialized before DQ execution"
            )

        if context.dry_run:
            return {
                "status": "DRY_RUN",
                "applies_to": apply_to,
                "rule_count": len(rules),
                "results": [
                    {
                        "rule_id": rule["rule_id"],
                        "status": "NOT_EXECUTED",
                        "severity": rule["severity"],
                    }
                    for rule in rules
                ],
            }

        if self.dq_executor is None:
            return {
                "status": "SKIPPED",
                "reason": "No dq_executor registered",
                "applies_to": apply_to,
                "rule_count": len(rules),
            }

        return self.dq_executor(
            df=dataframe_store[apply_to],
            dq_config=dq_cfg,
            context=context,
        )

    def _execute_write(
        self,
        pipeline: Mapping[str, Any],
        environment: Mapping[str, Any],
        context: ExecutionContext,
        dataframe_store: Mapping[str, Any],
    ) -> dict[str, Any]:
        write_cfg = dict(pipeline["write"])
        input_df_name = write_cfg["input_df"]
        target = _get_target_by_id(pipeline, write_cfg["target_id"])

        if input_df_name not in dataframe_store:
            raise PipelineExecutionError(
                f"write.input_df='{input_df_name}' was not produced before write phase"
            )

        if context.dry_run:
            return {
                "status": "DRY_RUN",
                "input_df": input_df_name,
                "target_id": target["target_id"],
                "mode": target["mode"],
                "path": target.get("path"),
                "table": f"{target.get('catalog')}.{target.get('schema')}.{target.get('table')}",
            }

        if self.writer is None:
            return {
                "status": "SKIPPED",
                "reason": "No writer registered",
                "input_df": input_df_name,
                "target_id": target["target_id"],
            }

        return self.writer(
            df=dataframe_store[input_df_name],
            target=target,
            environment=environment,
            context=context,
        )

    @staticmethod
    def _derive_pipeline_status(
        step_results: Iterable[StepResult],
        dq_result: Mapping[str, Any],
        write_result: Mapping[str, Any],
    ) -> str:
        step_statuses = {item.status for item in step_results}
        dq_status = dq_result.get("status")
        write_status = write_result.get("status")

        if "FAILED" in step_statuses or dq_status == "FAILED" or write_status == "FAILED":
            return "FAILED"
        if "DRY_RUN" in step_statuses or dq_status == "DRY_RUN" or write_status == "DRY_RUN":
            return "DRY_RUN"
        if dq_status == "WARNING":
            return "SUCCESS_WITH_WARNINGS"
        return "SUCCESS"


def _safe_row_count(df: Any) -> int | None:
    try:
        if hasattr(df, "count") and callable(df.count):
            return int(df.count())
    except Exception:  # pragma: no cover - best-effort metric only
        return None
    return None


def _get_source_by_id(pipeline: Mapping[str, Any], source_id: str) -> dict[str, Any]:
    for source in pipeline.get("sources", []):
        if source["source_id"] == source_id:
            return dict(source)
    raise PipelineExecutionError(f"Unknown source_id='{source_id}'")


def _get_target_by_id(pipeline: Mapping[str, Any], target_id: str) -> dict[str, Any]:
    for target in pipeline.get("targets", []):
        if target["target_id"] == target_id:
            return dict(target)
    raise PipelineExecutionError(f"Unknown target_id='{target_id}'")


def _hash_bundle(bundle: Mapping[str, Any]) -> str:
    payload = json.dumps(bundle, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
