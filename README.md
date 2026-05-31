# Senior Config-Driven ETL on Databricks

## Overview

This repository contains a **config-driven ETL framework** and a small set of example pipelines designed for a Databricks-style lakehouse environment.

The design goal is not only to run a few sample jobs, but to show a repeatable platform pattern:

- **Framework layer**: reusable execution engine, IO abstractions, transformation plugins, DQ executor, audit/control logging.
- **Pipeline definition layer**: declarative YAML configs that define sources, steps, validations, targets, and runtime parameters.
- **Orchestration layer**: Airflow/Databricks orchestration generated from templates, without business logic embedded in the DAGs.

This structure aligns with the assessment goals: configurable pipelines, reusable plugins, data quality controls, auditability, replayability, and clear trade-offs.

---

## Repository layout

```text
framework/           # reusable engine and plugins
configs/             # environment, pipeline, schema, rulepack configs
orchestration/       # airflow/databricks orchestration layer
templates/           # Jinja templates for DAG generation / SQL rendering
tests/               # unit and integration tests
sample_data/         # sample input datasets + generator
docs/                # architecture and design notes
```

---

## Architecture summary

The solution is intentionally split into **three independent layers**:

### 1) Framework
The framework is responsible for:

- loading and validating configs
- creating runtime context
- reading input datasets
- executing transformation plugins
- running data quality rules
- writing targets
- recording run/step/rule metadata

The framework should be stable across many pipelines.

### 2) Pipeline definitions
A pipeline is declared in YAML. The config describes:

- source definitions
- transformation steps
- DQ rules or included rulepacks
- targets
- runtime parameters
- stage / layer intent (silver, gold, etc.)

Adding a new pipeline should be possible **without changing framework code**.

### 3) Orchestration
Airflow and Databricks wrappers only orchestrate execution:

- render pipeline-specific DAGs from a template
- pass runtime arguments
- trigger the generic framework entrypoint
- handle schedule/retries/alerts

Business logic stays outside of Airflow.

---

## End-to-end flow

1. A pipeline config is selected.
2. Environment config and runtime overrides are loaded.
3. Configs are validated before Spark execution starts.
4. The executor reads sources and materializes named DataFrames.
5. Transformation plugins run in the declared order.
6. DQ rules execute against configured datasets.
7. Target datasets are written to Delta/Parquet.
8. Run control, step control, and DQ audit records are persisted.

---

## Local run instructions

### Prerequisites

- Python 3.12
- Java 17+
- local virtual environment
- PySpark installed
- optionally `delta-spark` if you want Delta output locally

Example setup:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install pydantic PyYAML pytest jinja2 pyspark delta-spark
```

### Generate sample data

```bash
python sample_data/generate_sample_data.py
```

This creates small but realistic banking-style datasets:

- clients
- credits
- collaterals
- market_info

with a few intentional quality issues for rule testing.

### Run tests

```bash
pytest -q
```

### Run a pipeline locally

Example:

```bash
python -m framework.entrypoints.run_pipeline \
  --pipeline-id clients_curated \
  --environment dev \
  --runtime-file configs/runtime/clients_curated.yaml
```

For orchestration rendering:

```bash
python -m framework.entrypoints.render_dag \
  --pipeline-config configs/pipelines/clients_curated.yaml \
  --environment-config configs/environments/dev.yaml \
  --environment dev \
  --output-path orchestration/airflow/generated/clients_curated_dag.py
```

---

## Sample data

The sample datasets are intentionally small and slightly imperfect, so the framework can demonstrate:

- type casting
- deduplication
- null checks
- accepted value checks
- numeric range checks
- multi-source joins for a gold mart

The included generator writes deterministic files so local runs are reproducible.

---

## Control and audit model

Three control entities are recommended.

### `etl_run_control`
One record per pipeline run. Typical fields:

- `run_id`
- `pipeline_id`
- `environment`
- `config_hash`
- `business_date`
- `trigger_type`
- `start_ts`
- `end_ts`
- `status`
- `error_summary`

### `etl_step_control`
One record per executed step:

- `run_id`
- `step_id`
- `step_type`
- `input_dataset`
- `output_dataset`
- `input_count`
- `output_count`
- `start_ts`
- `end_ts`
- `status`

### `etl_dq_results`
One record per executed DQ rule:

- `run_id`
- `pipeline_id`
- `dataset_name`
- `rule_id`
- `rule_type`
- `severity`
- `status`
- `metric_value`
- `failed_count`
- `details`
- `execution_ts`

This allows controlled failure handling, explainability, and replay support.

---

## Replay and reproducibility strategy

For repeatable execution, each run should capture:

- `run_id`
- exact pipeline config version or config hash
- environment name
- runtime overrides
- source file list or source snapshot reference
- business date / processing window
- output target locations

This makes reruns deterministic enough for audit and debugging.

---

## How to add a new pipeline

1. Add a new YAML file in `configs/pipelines/`.
2. Reuse existing transformation and DQ rule types where possible.
3. Add or reference rulepacks in `configs/dq_rulepacks/`.
4. Run config validation.
5. Run the pipeline locally.
6. Render its orchestration DAG.

No framework changes should be required for a normal new pipeline.

---

## How to add a new transformation

1. Implement a class under `framework/transformations/`.
2. Register it in the transformation registry.
3. Add a unit test.
4. Reference the new type in a pipeline config.

---

## How to add a new DQ rule

1. Implement the rule under `framework/dq/`.
2. Register it in the DQ registry.
3. Add a focused rule test.
4. Use it from a rulepack or inline config.

---

## Key design trade-offs

### Why PySpark + plugins instead of SQL-only
A SQL-only engine can look elegant at first, but plugin-based PySpark is easier to test, extend, and debug. SQL remains available as an optional step type.

### Why declarative config, but not a full DSL
The config should describe **what** to do, not become a programming language. This keeps pipelines readable and auditable.

### Why separate framework and orchestration
Airflow should not hold business logic. Keeping orchestration thin improves reuse and lowers maintenance cost.

### Why Delta/Parquet outputs
These formats are easy to test locally, align with Databricks lakehouse patterns, and support structured downstream processing.

### Why limited lineage
The prototype emphasizes **run-level, step-level, and dataset-level** traceability rather than a universal row-level lineage engine. That is a deliberate trade-off in favor of clarity and delivery speed.

### Why a small but realistic sample domain
A compact banking-style dataset is enough to demonstrate joins, rule failures, audit logging, and a gold mart without drowning the solution in domain noise.

---

## Submission checklist

- [x] config-driven pipelines
- [x] reusable transformation plugins
- [x] reusable DQ subsystem
- [x] tests
- [x] sample data + generator
- [x] orchestration template + rendered DAGs
- [x] architecture diagram
- [x] audit/control design notes

