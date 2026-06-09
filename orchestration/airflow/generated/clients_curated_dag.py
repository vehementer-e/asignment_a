"""
Auto-generated Airflow DAG for pipeline: clients_curated
Environment: dev
Generated from config-driven template.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="clients_curated_dag",
    description="Curate raw client records into a standardized silver dataset.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['clients_curated', 'dev', 'config-driven'],
)

databricks_task = DatabricksSubmitRunOperator(
    task_id="run_clients_curated",
    databricks_conn_id="databricks_default",
    existing_cluster_id="REPLACE_ME",
    notebook_task={
        "notebook_path": "/Shared/run_config_driven_pipeline",
        "base_parameters": {
            "pipeline_id": "clients_curated",
            "environment": "dev",
            "runtime_json": '{}',
            "config_root": "configs",
        },
    },
    libraries=[],
    timeout_seconds=3600,
    dag=dag,
)

databricks_task