from __future__ import annotations

from pathlib import Path

from framework.config.loader import ConfigLoader
from framework.config.merger import build_execution_bundle
from framework.execution.pipeline_executor import PipelineExecutor


CONFIG_ROOT = Path(__file__).resolve().parents[2] / "configs"


def test_pipeline_executor_contracts_are_compatible() -> None:
    loader = ConfigLoader(config_root=CONFIG_ROOT)
    loaded_bundle = loader.load_bundle(
        pipeline_id="clients_curated",
        environment="dev",
        runtime_file=CONFIG_ROOT / "runtime" / "example_runtime_params.yaml",
    )

    execution_bundle = build_execution_bundle(loaded_bundle)
    summary = PipelineExecutor().execute(execution_bundle, dry_run=True)

    assert summary["pipeline_id"] == "clients_curated"
    assert summary["environment"] == "dev"
    assert summary["dry_run"] is True
    assert summary["status"] in {"SUCCESS", "SUCCESS_WITH_WARNINGS"}
