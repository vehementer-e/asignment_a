from pathlib import Path
from pprint import pprint

from framework.config.loader import ConfigLoader
from framework.config.merger import build_execution_bundle, merge_runtime_values


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent
    config_root = repo_root / "configs"

    loader = ConfigLoader(config_root=config_root)
    bundle = loader.load_bundle(
        pipeline_id="clients_curated",
        environment="dev",
        runtime_file=config_root / "runtime" / "example_runtime_params.yaml",
    )

    execution_bundle = build_execution_bundle(
        pipeline=bundle["pipeline"],
        environment=bundle["environment"],
        runtime_invocation=bundle["runtime_invocation"],
        rulepacks=bundle["rulepacks"],
    )

    print("Effective runtime:")
    pprint(merge_runtime_values(bundle["pipeline"], bundle["environment"], bundle["runtime_invocation"]))
    print("\nResolved target path:")
    print(bundle["resolved_pipeline"]["targets"][0]["path"])
    print("\nEffective DQ rule ids:")
    pprint([r["rule_id"] for r in execution_bundle["pipeline"]["dq"]["effective_rules"]])
