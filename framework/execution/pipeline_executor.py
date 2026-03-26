
from __future__ import annotations

import logging
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from framework.audit.control_logger import ControlLogger
from framework.audit.dq_audit_logger import DQAuditLogger
from framework.audit.run_metadata import RunMetadataBuilder
from framework.dq.executor import DQExecutor
from framework.io.readers import ReaderRegistry
from framework.io.writers import WriterRegistry
from framework.transformations.registry import TransformationRegistry


logger = logging.getLogger(__name__)


class PipelineExecutor:
    """
    Generic config-driven pipeline executor.

    Expected bundle shape (from config loader / merger layer):
    {
        "pipeline": <PipelineConfig>,
        "environment": <EnvironmentConfig>,
        "runtime": {...},
        "effective_dq_rules": [...],
    }

    The executor is intentionally glue-code heavy and business-logic light.
    """

    def __init__(
        self,
        reader_registry: Optional[ReaderRegistry] = None,
        writer_registry: Optional[WriterRegistry] = None,
        transformation_registry: Optional[TransformationRegistry] = None,
        dq_executor: Optional[DQExecutor] = None,
        control_logger: Optional[ControlLogger] = None,
        dq_audit_logger: Optional[DQAuditLogger] = None,
        run_metadata_builder: Optional[RunMetadataBuilder] = None,
    ) -> None:
        self.reader_registry = reader_registry or ReaderRegistry.with_defaults()
        self.writer_registry = writer_registry or WriterRegistry.with_defaults()
        self.transformation_registry = transformation_registry or TransformationRegistry.with_defaults()
        self.dq_executor = dq_executor or DQExecutor.with_defaults()
        self.control_logger = control_logger or ControlLogger()
        self.dq_audit_logger = dq_audit_logger or DQAuditLogger()
        self.run_metadata_builder = run_metadata_builder or RunMetadataBuilder()

    def execute(self, execution_bundle: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
        pipeline = execution_bundle["pipeline"]
        environment = execution_bundle["environment"]
        runtime = execution_bundle.get("runtime", {}) or {}
        effective_dq_rules = execution_bundle.get("effective_dq_rules", []) or []

        pipeline_id = getattr(pipeline, "pipeline_id", "unknown_pipeline")
        env_name = getattr(environment, "name", "unknown_env")
        run_id = runtime.get("run_id") or self.run_metadata_builder.generate_run_id(pipeline_id)

        metadata = self.run_metadata_builder.build(
            pipeline=pipeline,
            environment=environment,
            runtime=runtime,
            run_id=run_id,
            effective_dq_rules=effective_dq_rules,
        )

        self.control_logger.log_run_start(
            pipeline_id=pipeline_id,
            run_id=run_id,
            environment=env_name,
            metadata=metadata,
        )

        start_ts = self._utc_now()
        dataframes: Dict[str, Any] = {}
        step_results: List[Dict[str, Any]] = []
        dq_results: List[Dict[str, Any]] = []
        output_targets: List[Dict[str, Any]] = []
        warnings: List[str] = []

        failed = False
        failure_reason = None

        try:
            steps = list(getattr(pipeline, "steps", []) or [])

            for step in steps:
                step_id = getattr(step, "step_id", "unknown_step")
                step_type = getattr(step, "type", "unknown")
                step_start = self._utc_now()

                self.control_logger.log_step_start(
                    pipeline_id=pipeline_id,
                    run_id=run_id,
                    step_id=step_id,
                    step_type=step_type,
                )

                try:
                    if step_type == "read":
                        result = self._execute_read_step(
                            step=step,
                            pipeline=pipeline,
                            environment=environment,
                            runtime=runtime,
                            dry_run=dry_run,
                        )
                        dataframes[result["output_df"]] = result["dataframe"]

                    elif step_type == "write":
                        result = self._execute_write_step(
                            step=step,
                            pipeline=pipeline,
                            environment=environment,
                            runtime=runtime,
                            dataframes=dataframes,
                            dry_run=dry_run,
                        )
                        output_targets.append(result)

                    elif step_type == "dq":
                        dataset_name = getattr(step, "input_df", None) or getattr(step, "params", {}).get("input_df")
                        dataset_df = dataframes.get(dataset_name)

                        result = self._execute_dq_step(
                            step=step,
                            dataset_name=dataset_name,
                            dataset_df=dataset_df,
                            rules=effective_dq_rules,
                            runtime=runtime,
                            dry_run=dry_run,
                        )
                        dq_results.extend(result["dq_results"])
                        self.dq_audit_logger.log_many(
                            pipeline_id=pipeline_id,
                            run_id=run_id,
                            dataset_name=dataset_name,
                            dq_results=result["dq_results"],
                        )
                        warnings.extend(result.get("warnings", []))

                    else:
                        result = self._execute_transformation_step(
                            step=step,
                            runtime=runtime,
                            dataframes=dataframes,
                            dry_run=dry_run,
                        )
                        output_df_name = result["output_df"]
                        dataframes[output_df_name] = result["dataframe"]

                    step_end = self._utc_now()
                    enriched_result = {
                        **result,
                        "step_id": step_id,
                        "step_type": step_type,
                        "started_at": step_start,
                        "finished_at": step_end,
                        "status": "SUCCESS",
                    }
                    step_results.append(enriched_result)

                    self.control_logger.log_step_end(
                        pipeline_id=pipeline_id,
                        run_id=run_id,
                        step_id=step_id,
                        status="SUCCESS",
                        details=self._serialise(enriched_result),
                    )

                except Exception as exc:  # noqa: BLE001
                    step_end = self._utc_now()
                    failed = True
                    failure_reason = f"{step_id}: {exc}"

                    error_result = {
                        "step_id": step_id,
                        "step_type": step_type,
                        "started_at": step_start,
                        "finished_at": step_end,
                        "status": "FAILED",
                        "error": str(exc),
                    }
                    step_results.append(error_result)

                    self.control_logger.log_step_end(
                        pipeline_id=pipeline_id,
                        run_id=run_id,
                        step_id=step_id,
                        status="FAILED",
                        details=self._serialise(error_result),
                    )
                    raise

            final_status = "SUCCESS_WITH_WARNINGS" if warnings else "SUCCESS"

        except Exception as exc:  # noqa: BLE001
            logger.exception("Pipeline execution failed for %s", pipeline_id)
            final_status = "FAILED"
            failed = True
            failure_reason = failure_reason or str(exc)

        end_ts = self._utc_now()
        summary = {
            "pipeline_id": pipeline_id,
            "environment": env_name,
            "run_id": run_id,
            "status": final_status,
            "failed": failed,
            "failure_reason": failure_reason,
            "dry_run": dry_run,
            "started_at": start_ts,
            "finished_at": end_ts,
            "step_results": step_results,
            "dq_results": dq_results,
            "warnings": warnings,
            "output_targets": output_targets,
            "datasets_materialized": sorted(list(dataframes.keys())),
            "metadata": metadata,
        }

        self.control_logger.log_run_end(
            pipeline_id=pipeline_id,
            run_id=run_id,
            status=final_status,
            details=self._serialise(summary),
        )
        return summary

    def _execute_read_step(self, step: Any, pipeline: Any, environment: Any, runtime: Dict[str, Any], dry_run: bool) -> Dict[str, Any]:
        params = getattr(step, "params", {}) or {}
        source_id = params["source_id"]
        output_df = getattr(step, "output_df", None) or source_id

        source = self._find_by_id(getattr(pipeline, "sources", []) or [], source_id, "source_id")
        source_format = getattr(source, "format", None)

        if dry_run:
            dataframe = {"__dry_run__": True, "source_id": source_id, "format": source_format}
            row_count = None
        else:
            reader = self.reader_registry.get(source_format)
            dataframe = reader.read(source=source, environment=environment, runtime=runtime)
            row_count = self._safe_count(dataframe)

        return {
            "action": "read",
            "source_id": source_id,
            "output_df": output_df,
            "dataframe": dataframe,
            "row_count": row_count,
            "format": source_format,
        }

    def _execute_transformation_step(self, step: Any, runtime: Dict[str, Any], dataframes: Dict[str, Any], dry_run: bool) -> Dict[str, Any]:
        step_type = getattr(step, "type", None)
        input_df_name = getattr(step, "input_df", None)
        output_df_name = getattr(step, "output_df", None) or input_df_name

        if not input_df_name:
            raise ValueError(f"Transformation step '{getattr(step, 'step_id', '?')}' must declare input_df")

        input_df = dataframes[input_df_name]
        params = getattr(step, "params", {}) or {}

        if dry_run:
            dataframe = {
                "__dry_run__": True,
                "transformation": step_type,
                "input_df": input_df_name,
                "params": params,
            }
            row_count = None
        else:
            transformation = self.transformation_registry.get(step_type)
            dataframe = transformation.apply(
                df=input_df,
                params=params,
                context={
                    "runtime": runtime,
                    "dataframes": dataframes,
                    "step": step,
                },
            )
            row_count = self._safe_count(dataframe)

        return {
            "action": "transform",
            "transformation_type": step_type,
            "input_df": input_df_name,
            "output_df": output_df_name,
            "dataframe": dataframe,
            "row_count": row_count,
        }

    def _execute_dq_step(
        self,
        step: Any,
        dataset_name: Optional[str],
        dataset_df: Any,
        rules: List[Any],
        runtime: Dict[str, Any],
        dry_run: bool,
    ) -> Dict[str, Any]:
        if not dataset_name:
            raise ValueError(f"DQ step '{getattr(step, 'step_id', '?')}' requires input_df")
        if dataset_df is None:
            raise ValueError(f"DQ step '{getattr(step, 'step_id', '?')}' references missing dataset '{dataset_name}'")

        step_rule_ids = (getattr(step, "params", {}) or {}).get("rule_ids")
        selected_rules = self._select_dq_rules(rules, step_rule_ids)

        if dry_run:
            dq_results = [
                {
                    "rule_id": getattr(rule, "rule_id", "unknown"),
                    "rule_type": getattr(rule, "rule_type", "unknown"),
                    "status": "SKIPPED",
                    "severity": getattr(rule, "severity", "warn"),
                    "message": "DQ skipped because executor is in dry_run mode",
                }
                for rule in selected_rules
            ]
            warnings = []
        else:
            dq_results = self.dq_executor.execute_many(
                df=dataset_df,
                rules=selected_rules,
                context={"runtime": runtime, "dataset_name": dataset_name},
            )
            warnings = self._collect_rule_warnings(dq_results)

        return {
            "action": "dq",
            "dataset_name": dataset_name,
            "dq_results": [self._serialise(item) for item in dq_results],
            "warnings": warnings,
        }

    def _execute_write_step(
        self,
        step: Any,
        pipeline: Any,
        environment: Any,
        runtime: Dict[str, Any],
        dataframes: Dict[str, Any],
        dry_run: bool,
    ) -> Dict[str, Any]:
        params = getattr(step, "params", {}) or {}
        input_df_name = getattr(step, "input_df", None)
        target_id = params["target_id"]

        if input_df_name not in dataframes:
            raise ValueError(f"Write step '{getattr(step, 'step_id', '?')}' references missing dataframe '{input_df_name}'")

        target = self._find_by_id(getattr(pipeline, "targets", []) or [], target_id, "target_id")
        target_format = getattr(target, "format", None)
        dataset = dataframes[input_df_name]

        if dry_run:
            row_count = None
        else:
            writer = self.writer_registry.get(target_format)
            writer.write(df=dataset, target=target, environment=environment, runtime=runtime)
            row_count = self._safe_count(dataset)

        return {
            "action": "write",
            "input_df": input_df_name,
            "target_id": target_id,
            "format": target_format,
            "mode": getattr(target, "mode", None),
            "row_count": row_count,
        }

    @staticmethod
    def _find_by_id(items: List[Any], target_id: str, attribute: str) -> Any:
        for item in items:
            if getattr(item, attribute, None) == target_id:
                return item
        raise ValueError(f"Unknown {attribute} '{target_id}'")

    @staticmethod
    def _safe_count(df: Any) -> Optional[int]:
        try:
            return int(df.count())
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _serialise(value: Any) -> Any:
        if is_dataclass(value):
            return asdict(value)
        if isinstance(value, list):
            return [PipelineExecutor._serialise(item) for item in value]
        if isinstance(value, dict):
            return {key: PipelineExecutor._serialise(item) for key, item in value.items()}
        if hasattr(value, "model_dump"):
            return value.model_dump()
        return value

    @staticmethod
    def _select_dq_rules(rules: List[Any], rule_ids: Optional[List[str]]) -> List[Any]:
        if not rule_ids:
            return list(rules)
        allowed = set(rule_ids)
        return [rule for rule in rules if getattr(rule, "rule_id", None) in allowed]

    @staticmethod
    def _collect_rule_warnings(results: List[Any]) -> List[str]:
        warnings: List[str] = []
        for item in results:
            result = PipelineExecutor._serialise(item)
            status = result.get("status")
            severity = result.get("severity")
            rule_id = result.get("rule_id")
            if status in {"FAILED", "WARNING"} and severity in {"warn", "warning"}:
                warnings.append(f"{rule_id}: {status}")
        return warnings
