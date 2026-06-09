# Orchestration bundle

Included:
- `templates/airflow/databricks_pipeline_dag.py.j2` — Jinja DAG template
- `framework/entrypoints/render_dag.py` — render one DAG from config
- `orchestration/airflow/dag_generator.py` — generate all DAGs from `configs/pipelines`
- `orchestration/databricks/task_entrypoint.py` — thin Databricks wrapper that delegates to `framework.entrypoints.run_pipeline`
- `orchestration/airflow/generated/*.py` — sample rendered DAG outputs

Design rule: orchestration stays thin and contains no ETL business logic.
