
"""
Example wiring for the integrated executor.

This file is illustrative: it assumes the rest of the generated project
structure exists in the Python path.
"""

from pathlib import Path

from framework.config.loader import ConfigLoader
from framework.config.merger import build_execution_bundle
from framework.execution.pipeline_executor import PipelineExecutor


def main() -> None:
    loader = ConfigLoader(config_root=Path("configs"))
    loaded_bundle = loader.load_bundle(
        pipeline_id="clients_curated",
        environment="dev",
        runtime_file="configs/runtime/example_runtime_params.yaml",
    )
    execution_bundle = build_execution_bundle(loaded_bundle)
    summary = PipelineExecutor().execute(execution_bundle, dry_run=True)
    print(summary["status"])
    print(summary["datasets_materialized"])


if __name__ == "__main__":
    main()
