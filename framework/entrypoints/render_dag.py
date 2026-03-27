from __future__ import annotations

import argparse
import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined
import yaml


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def build_template_context(pipeline_cfg: dict, env_cfg: dict, environment: str) -> dict:
    pipeline_id = pipeline_cfg["pipeline_id"]
    orchestration = pipeline_cfg.get("orchestration", {})
    airflow_cfg = env_cfg.get("airflow", {})
    databricks_cfg = env_cfg.get("databricks", {})
    runtime_defaults = pipeline_cfg.get("runtime", {}).get("defaults", {})
    return {
        "pipeline_id": pipeline_id,
        "dag_id": orchestration.get("dag_id", f"{pipeline_id}_dag"),
        "description": pipeline_cfg.get("description", f"Generated DAG for {pipeline_id}"),
        "owner": airflow_cfg.get("owner", "data-platform"),
        "retries": orchestration.get("retries", airflow_cfg.get("retries", 1)),
        "retry_delay_minutes": orchestration.get("retry_delay_minutes", airflow_cfg.get("retry_delay_minutes", 5)),
        "start_year": orchestration.get("start_date", {}).get("year", 2026),
        "start_month": orchestration.get("start_date", {}).get("month", 1),
        "start_day": orchestration.get("start_date", {}).get("day", 1),
        "schedule": orchestration.get("schedule", "@daily"),
        "tags": orchestration.get("tags", [pipeline_id, environment, "config-driven"]),
        "databricks_conn_id": airflow_cfg.get("databricks_conn_id", "databricks_default"),
        "existing_cluster_id": databricks_cfg.get("existing_cluster_id", "REPLACE_ME"),
        "notebook_path": databricks_cfg.get("notebook_path", "/Shared/run_config_driven_pipeline"),
        "timeout_seconds": orchestration.get("timeout_seconds", 3600),
        "libraries": databricks_cfg.get("libraries", []),
        "environment": environment,
        "runtime_json": json.dumps(runtime_defaults),
        "config_root": env_cfg.get("config_root", "configs"),
    }


def render_dag_template(template_dir: Path, template_name: str, context: dict) -> str:
    env = Environment(loader=FileSystemLoader(str(template_dir)), undefined=StrictUndefined, trim_blocks=True, lstrip_blocks=True)
    template = env.get_template(template_name)
    return template.render(**context)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render Airflow DAG from pipeline and environment configs.")
    parser.add_argument("--pipeline-config", required=True)
    parser.add_argument("--environment-config", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--template-dir", default="templates/airflow")
    parser.add_argument("--template-name", default="databricks_pipeline_dag.py.j2")
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    pipeline_cfg = load_yaml(Path(args.pipeline_config))
    env_cfg = load_yaml(Path(args.environment_config))
    context = build_template_context(pipeline_cfg, env_cfg, args.environment)
    rendered = render_dag_template(Path(args.template_dir), args.template_name, context)
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    print(f"Rendered DAG written to {output_path}")


if __name__ == "__main__":
    main()
