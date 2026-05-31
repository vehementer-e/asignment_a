from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Severity(str, Enum):
    FAIL = "fail"
    WARN = "warn"
    CONTINUE = "continue"


class Layer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class Engine(str, Enum):
    PYSPARK = "pyspark"


class SourceType(str, Enum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    DELTA = "delta"
    DELTA_TABLE = "delta_table"


class TargetType(str, Enum):
    DELTA = "delta"


class WriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"


class StepType(str, Enum):
    READ = "read"
    TRANSFORMATION = "transformation"


class RuntimeParamType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    DATE = "date"


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class RuntimeParameter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: RuntimeParamType
    required: bool
    default: Any | None = None


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str
    source_type: SourceType
    path: Optional[str] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None
    table: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)
    logical_name: Optional[str] = None
    alias: Optional[str] = None

    @model_validator(mode="after")
    def validate_location(self) -> "SourceConfig":
        if self.source_type in {SourceType.CSV, SourceType.JSON, SourceType.PARQUET, SourceType.DELTA}:
            if not self.path:
                raise ValueError(f"source '{self.source_id}' requires 'path' for source_type='{self.source_type.value}'")
        if self.source_type == SourceType.DELTA_TABLE:
            missing = [name for name in ("catalog", "schema", "table") if not getattr(self, name)]
            if missing:
                raise ValueError(
                    f"source '{self.source_id}' requires {missing} for source_type='delta_table'"
                )
        return self


class TargetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_id: str
    target_type: TargetType
    mode: WriteMode
    catalog: Optional[str] = None
    schema: Optional[str] = None
    table: Optional[str] = None
    path: Optional[str] = None

    @model_validator(mode="after")
    def validate_location(self) -> "TargetConfig":
        missing = [name for name in ("catalog", "schema", "table", "path") if not getattr(self, name)]
        if missing:
            raise ValueError(f"target '{self.target_id}' requires {missing}")
        return self


class OrderBySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    column: str
    direction: SortDirection = SortDirection.ASC


class DQRuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rule_id: str
    rule_type: Literal[
        "not_null",
        "uniqueness",
        "accepted_values",
        "numeric_range",
        "freshness",
        "referential_integrity",
        "row_count_min",
    ]
    params: Dict[str, Any] = Field(default_factory=dict)
    severity: Severity


class DQConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    apply_to: str
    include_rulepacks: List[str] = Field(default_factory=list)
    rules: List[DQRuleConfig] = Field(default_factory=list)


class WriteConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_df: str
    target_id: str


class PostWriteCheckConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    check_type: str
    expected_relation: Optional[str] = None


class ReadStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step_id: str
    step_type: Literal["read"]
    source_id: str
    output_df: str


class TransformationStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step_id: str
    step_type: Literal["transformation"]
    transformation: str
    input_df: str
    output_df: str
    right_df: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)


StepConfig = Union[ReadStepConfig, TransformationStepConfig]


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str
    version: int = Field(ge=1)
    owner: str
    layer: Layer
    description: Optional[str] = None
    engine: Engine
    schedule: Optional[str] = None
    runtime_parameters: List[RuntimeParameter] = Field(default_factory=list)
    sources: List[SourceConfig]
    targets: List[TargetConfig]
    steps: List[StepConfig]
    dq: DQConfig
    write: WriteConfig
    post_write_checks: List[PostWriteCheckConfig] = Field(default_factory=list)


class DatabricksConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_url: str
    job_cluster_policy: Optional[str] = None
    default_catalog: str
    default_schema: str
    checkpoint_root: str


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    landing_root: str
    bronze_root: str
    silver_root: str
    gold_root: str
    audit_root: str


class ControlTablesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    catalog: str
    schema: str
    run_control_table: str
    step_control_table: str
    dq_results_table: str


class EnvironmentConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: Literal["local", "dev", "test", "prod"]
    cloud: Literal["local", "azure"]
    databricks: DatabricksConfig
    storage: StorageConfig
    control_tables: ControlTablesConfig
    secrets: Dict[str, Any] = Field(default_factory=dict)
    runtime_defaults: Dict[str, Any] = Field(default_factory=dict)
    feature_flags: Dict[str, bool] = Field(default_factory=dict)


class RuntimeInvocation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str
    environment: Literal["local", "dev", "test", "prod"]
    runtime: Dict[str, Any] = Field(default_factory=dict)


class RulePackConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rulepack_id: str
    version: int = Field(ge=1)
    rules: List[DQRuleConfig] = Field(default_factory=list)


class LoadedConfigBundle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_config: PipelineConfig
    environment_config: EnvironmentConfig
    runtime_invocation: RuntimeInvocation
    rulepacks: Dict[str, RulePackConfig] = Field(default_factory=dict)
    runtime_overrides: Dict[str, Any] = Field(default_factory=dict)
    resolved_pipeline_config: Dict[str, Any] = Field(default_factory=dict)


class ExecutionBundle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_config: PipelineConfig
    environment_config: EnvironmentConfig
    merged_pipeline_config: Dict[str, Any] = Field(default_factory=dict)
    merged_dq_rules: List[DQRuleConfig] = Field(default_factory=list)
    resolved_runtime: Dict[str, Any] = Field(default_factory=dict)
    source_locations: Dict[str, str] = Field(default_factory=dict)
    target_locations: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
