
"""
Example wiring for the integrated executor.

This file is illustrative: it assumes the rest of the generated project
structure exists in the Python path.
"""

from framework.config.loader import ConfigLoader
from framework.config.validator import ConfigValidator
from framework.execution.pipeline_executor import PipelineExecutor


def main() -> None:
    loader = ConfigLoader(repo_root=".")
    bundle = loader.load_bundle(
        pipeline_id="clients_curated",
        environment="dev",
        runtime_file="configs/runtime/example_runtime_params.yaml",
    )

    ConfigValidator().validate_bundle(bundle)

    summary = PipelineExecutor().execute(bundle, dry_run=True)
    print(summary["status"])
    print(summary["datasets_materialized"])


if __name__ == "__main__":
    main()
