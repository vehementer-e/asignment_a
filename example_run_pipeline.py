from framework.config.loader import ConfigLoader
from framework.config.merger import build_execution_bundle
from framework.execution.pipeline_executor import PipelineExecutor


# Demo only: validates configs, merges runtime, and performs a dry-run execution.
loader = ConfigLoader(config_root="configs")
loaded = loader.load_bundle(
    pipeline_id="clients_curated",
    environment="dev",
    runtime_overrides={"business_date": "2026-03-26", "run_id": "demo-run-001"},
)

bundle = build_execution_bundle(
    pipeline=loaded["pipeline"],
    environment=loaded["environment"],
    runtime_invocation=loaded["runtime_invocation"],
    rulepacks=loaded["rulepacks"],
)

executor = PipelineExecutor()
summary = executor.execute(bundle, dry_run=True)
print(summary["status"])
print(summary["materialized_dataframes"])
