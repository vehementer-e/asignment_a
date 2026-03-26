from framework.config.validator import (
    load_pipeline_config,
    load_environment_config,
    load_runtime_invocation,
    discover_rulepack_ids,
    validate_pipeline_semantics,
    validate_runtime_against_pipeline,
)

pipeline = load_pipeline_config("configs/pipelines/clients_curated.yaml")
env = load_environment_config("configs/environments/dev.yaml")
runtime = load_runtime_invocation("configs/runtime/example_runtime_params.yaml")
rulepacks = discover_rulepack_ids("configs/dq_rulepacks")

validate_pipeline_semantics(
    pipeline,
    available_rulepacks=rulepacks,
    allowed_transformations=[
        "select_columns",
        "rename_columns",
        "cast_columns",
        "filter_rows",
        "join",
        "derive_columns",
        "deduplicate",
        "sql_transform",
    ],
)
validate_runtime_against_pipeline(runtime, pipeline, env)
print("All config checks passed.")
