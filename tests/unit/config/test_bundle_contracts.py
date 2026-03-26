from __future__ import annotations

from pathlib import Path

from framework.config.loader import ConfigLoader
from framework.config.merger import build_execution_bundle
from framework.config.models import ExecutionBundle, LoadedConfigBundle
from framework.execution.pipeline_executor import PipelineExecutor


CONFIG_ROOT = Path(__file__).resolve().parents[3] / "configs"


def test_loader_returns_typed_bundle() -> None:
    loader = ConfigLoader(config_root=CONFIG_ROOT)

    bundle = loader.load_bundle(
        pipeline_id="clients_curated",
        environment="dev",
        runtime_file=CONFIG_ROOT / "runtime" / "example_runtime_params.yaml",
        runtime_overrides={"business_date": "2026-03-26"},
    )

    assert isinstance(bundle, LoadedConfigBundle)
    assert bundle.pipeline_config.pipeline_id == "clients_curated"
    assert bundle.environment_config.environment == "dev"
    assert "common_validations" in bundle.rulepacks
    assert bundle.runtime_overrides["business_date"] == "2026-03-26"


def test_merger_builds_execution_bundle_for_executor() -> None:
    loader = ConfigLoader(config_root=CONFIG_ROOT)
    loaded_bundle = loader.load_bundle(
        pipeline_id="clients_curated",
        environment="dev",
        runtime_file=CONFIG_ROOT / "runtime" / "example_runtime_params.yaml",
    )

    execution_bundle = build_execution_bundle(loaded_bundle)

    assert isinstance(execution_bundle, ExecutionBundle)
    assert execution_bundle.resolved_runtime["run_id"] == "manual_20260324_001"
    assert execution_bundle.merged_pipeline_config["targets"][0]["path"].startswith("abfss://silver")
    assert execution_bundle.source_locations["clients_raw"].endswith("/clients/")
    assert execution_bundle.target_locations["clients_silver"].endswith("/risk/clients_silver")
    assert len(execution_bundle.merged_dq_rules) > 0


def test_executor_accepts_premerged_execution_bundle() -> None:
    loader = ConfigLoader(config_root=CONFIG_ROOT)
    loaded_bundle = loader.load_bundle(
        pipeline_id="clients_curated",
        environment="dev",
        runtime_file=CONFIG_ROOT / "runtime" / "example_runtime_params.yaml",
    )
    execution_bundle = build_execution_bundle(loaded_bundle)

    summary = PipelineExecutor().execute(execution_bundle, dry_run=True)

    assert summary["status"] == "SUCCESS"
    assert summary["pipeline_id"] == "clients_curated"
    assert summary["environment"] == "dev"
