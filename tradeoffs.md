# Design Trade-offs

## 1. Config-driven, not code-driven
A new normal pipeline should be created via YAML only. This lowers repetition and supports reuse. The trade-off is that configs need strong validation to avoid shifting mistakes from code into metadata.

## 2. PySpark plugins over pure SQL
PySpark plugins are more verbose than dynamic SQL templates, but they are easier to test and reason about. SQL remains available as an optional transformation type.

## 3. Thin orchestration
Airflow is used only to schedule and trigger work. This avoids hiding business logic in DAG files, at the cost of needing a separate rendering/generation layer.

## 4. Dataset-level traceability over row-level lineage
A true row-level lineage engine would be expensive and well beyond the scope of a compact prototype. The chosen design focuses on run-level, step-level, and dataset-level traceability.

## 5. Delta/Parquet outputs for portability
Delta is preferred in a Databricks target environment. Parquet remains useful for local runs and portability. The trade-off is that local Delta setup requires more care than plain CSV/Parquet.

## 6. Intentional simplification of domain scope
The sample domain is small on purpose. The focus is to demonstrate framework quality, not maximum business complexity.
