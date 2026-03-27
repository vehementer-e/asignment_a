from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
import yaml


def iter_pipeline_configs(pipelines_dir: Path):
    for path in sorted(pipelines_dir.glob("*.yaml")):
        yield path


def generate_one(render_script: Path, pipeline_config: Path, environment_config: Path, environment: str, output_dir: Path) -> None:
    with pipeline_config.open("r", encoding="utf-8") as f:
        pipeline_cfg = yaml.safe_load(f) or {}
    pipeline_id = pipeline_cfg["pipeline_id"]
    output_path = output_dir / f"{pipeline_id}_dag.py"
    cmd = [
        "python", str(render_script),
        "--pipeline-config", str(pipeline_config),
        "--environment-config", str(environment_config),
        "--environment", environment,
        "--output-path", str(output_path),
    ]
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Airflow DAGs for all pipeline configs.")
    parser.add_argument("--pipelines-dir", default="configs/pipelines")
    parser.add_argument("--environment-config", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--render-script", default="framework/entrypoints/render_dag.py")
    parser.add_argument("--output-dir", default="orchestration/airflow/generated")
    args = parser.parse_args()

    pipelines_dir = Path(args.pipelines_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for pipeline_config in iter_pipeline_configs(pipelines_dir):
        generate_one(Path(args.render_script), pipeline_config, Path(args.environment_config), args.environment, output_dir)
    print(f"DAG generation completed in {output_dir}")


if __name__ == "__main__":
    main()
