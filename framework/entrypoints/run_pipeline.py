from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from framework.config.loader import ConfigLoader
from framework.config.merger import build_execution_bundle, merge_runtime_values
from framework.config.validator import validate_pipeline_semantics, validate_runtime_against_pipeline
from framework.execution.pipeline_executor import PipelineExecutor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a config-driven pipeline through the generic framework entrypoint."
    )
    parser.add_argument("--config-root", default="configs", help="Root directory of config files")
    parser.add_argument("--pipeline-id", required=True, help="Pipeline identifier, e.g. clients_curated")
    parser.add_argument("--environment", required=True, choices=["dev", "test", "prod"])
    parser.add_argument("--runtime-file", help="Path to a runtime invocation yaml/json file")
    parser.add_argument(
        "--runtime-json",
        help="Inline runtime override JSON object, e.g. '{\"business_date\": \"2026-03-26\"}'",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate and simulate execution only")
    parser.add_argument(
        "--summary-out",
        help="Optional output path for execution summary JSON",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    runtime_overrides = _parse_runtime_json(args.runtime_json)

    loader = ConfigLoader(config_root=args.config_root)
    loaded = loader.load_bundle(
        pipeline_id=args.pipeline_id,
        environment=args.environment,
        runtime_file=args.runtime_file,
        runtime_overrides=runtime_overrides,
    )

    pipeline_cfg = loaded["pipeline"]
    environment_cfg = loaded["environment"]
    runtime_invocation = loaded["runtime_invocation"]
    rulepacks = loaded["rulepacks"]

    validate_pipeline_semantics(
        pipeline_cfg,
        available_rulepacks=rulepacks.keys(),
        allowed_transformations=loader.allowed_transformations,
    )
    validate_runtime_against_pipeline(runtime_invocation, pipeline_cfg, environment_cfg)

    merged_runtime = merge_runtime_values(pipeline_cfg, environment_cfg, runtime_invocation)
    logging.getLogger(__name__).info(
        "Effective runtime for pipeline '%s': %s",
        pipeline_cfg.pipeline_id,
        json.dumps(merged_runtime, indent=2, default=str),
    )

    bundle = build_execution_bundle(
        pipeline=pipeline_cfg,
        environment=environment_cfg,
        runtime_invocation=runtime_invocation,
        rulepacks=rulepacks,
    )

    executor = PipelineExecutor()
    summary = executor.execute(bundle, dry_run=args.dry_run)

    rendered_summary = json.dumps(summary, indent=2, default=str)
    print(rendered_summary)

    if args.summary_out:
        out_path = Path(args.summary_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered_summary + "\n", encoding="utf-8")

    return 0 if summary["status"] not in {"FAILED"} else 1


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _parse_runtime_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid --runtime-json payload: {exc}") from exc

    if not isinstance(payload, dict):
        raise SystemExit("--runtime-json must deserialize to an object/dictionary")
    return payload


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
