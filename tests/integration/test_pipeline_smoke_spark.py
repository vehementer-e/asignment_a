from __future__ import annotations

from types import SimpleNamespace

from framework.execution.pipeline_executor import PipelineExecutor


def test_pipeline_smoke_runs_real_spark_flow(tmp_path, spark) -> None:
    input_csv = tmp_path / "clients.csv"
    output_path = tmp_path / "clients_output"

    input_csv.write_text(
        "\n".join(
            [
                "ClientID,Name,ingestion_ts",
                "c1,Alice,2026-03-20",
                "c1,Alice Updated,2026-03-21",
                "c2,Bob,2026-03-21",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = SimpleNamespace(
        pipeline_id="smoke_clients_pipeline",
        sources=[
            SimpleNamespace(
                source_id="clients_csv",
                format="csv",
                path=str(input_csv),
                options={"header": True, "inferSchema": True},
            )
        ],
        targets=[
            SimpleNamespace(
                target_id="clients_output",
                format="parquet",
                mode="overwrite",
                path=str(output_path),
            )
        ],
        steps=[
            SimpleNamespace(step_id="read_clients", step_type="read", source_id="clients_csv", output_df="clients_raw"),
            SimpleNamespace(
                step_id="rename_columns",
                step_type="transformation",
                transformation="rename_columns",
                input_df="clients_raw",
                output_df="clients_renamed",
                params={"mappings": {"ClientID": "client_id", "Name": "name"}},
            ),
            SimpleNamespace(
                step_id="deduplicate_clients",
                step_type="transformation",
                transformation="deduplicate",
                input_df="clients_renamed",
                output_df="clients_final",
                params={
                    "keys": ["client_id"],
                    "order_by": [{"column": "ingestion_ts", "direction": "desc"}],
                    "keep": "first",
                },
            ),
            SimpleNamespace(
                step_id="dq_not_null_client_id",
                step_type="dq",
                input_df="clients_final",
                params={"rule_ids": ["client_id_not_null"]},
            ),
            SimpleNamespace(
                step_id="write_output",
                step_type="write",
                input_df="clients_final",
                target_id="clients_output",
            ),
        ],
    )
    environment = SimpleNamespace(environment="test")
    rule = SimpleNamespace(
        rule_id="client_id_not_null",
        rule_type="not_null",
        params={"column": "client_id"},
        severity="fail",
    )

    execution_bundle = {
        "pipeline": pipeline,
        "environment": environment,
        "runtime": {"spark": spark, "run_id": "smoke-test-run"},
        "effective_dq_rules": [rule],
    }

    summary = PipelineExecutor().execute(execution_bundle=execution_bundle, dry_run=False)

    assert summary["status"] == "SUCCESS"
    assert output_path.exists()

    output_rows = spark.read.parquet(str(output_path)).select("client_id", "name").collect()
    output = {(row["client_id"], row["name"]) for row in output_rows}
    assert output == {("c1", "Alice Updated"), ("c2", "Bob")}

    dq_result = summary["dq_results"][0]
    assert dq_result["rule_id"] == "client_id_not_null"
    assert dq_result["status"] == "PASSED"
