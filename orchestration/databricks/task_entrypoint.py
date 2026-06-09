from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Thin Databricks wrapper for config-driven ETL execution.")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--config-root", default="configs")
    parser.add_argument("--runtime-json", default="{}")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    runtime_payload = json.loads(args.runtime_json)
    runtime_tmp = Path("/tmp/runtime_overrides.json")
    runtime_tmp.write_text(json.dumps(runtime_payload), encoding="utf-8")

    cmd = [
        "python", "-m", "framework.entrypoints.run_pipeline",
        "--pipeline-id", args.pipeline_id,
        "--environment", args.environment,
        "--runtime-file", str(runtime_tmp),
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
