"""
Auto-generated Airflow DAG for pipeline: credit_risk_mart
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
    dag_id="credit_risk_mart_dag",
    description="Build a gold risk mart by combining curated clients and credits.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['credit_risk_mart', 'dev', 'config-driven'],
)

databricks_task = DatabricksSubmitRunOperator(
    task_id="run_credit_risk_mart",
    databricks_conn_id="databricks_default",
    existing_cluster_id="REPLACE_ME",
    notebook_task={
        "notebook_path": "/Shared/run_config_driven_pipeline",
        "base_parameters": {
            "pipeline_id": "credit_risk_mart",
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