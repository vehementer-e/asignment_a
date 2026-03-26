from __future__ import annotations

from framework.config.models import (
    DQConfig,
    DQRuleConfig,
    Engine,
    EnvironmentConfig,
    Layer,
    PipelineConfig,
    ReadStepConfig,
    RuntimeInvocation,
    RuntimeParameter,
    RuntimeParamType,
    Severity,
    SourceConfig,
    SourceType,
    TargetConfig,
    TargetType,
    TransformationStepConfig,
    WriteConfig,
    WriteMode,
)
from framework.config.validator import (
    ConfigValidationError,
    validate_pipeline_semantics,
    validate_runtime_against_pipeline,
)


def _build_environment() -> EnvironmentConfig:
    return EnvironmentConfig.model_validate(
        {
            'environment': 'dev',
            'cloud': 'azure',
            'databricks': {
                'workspace_url': 'https://example.azuredatabricks.net',
                'default_catalog': 'main',
                'default_schema': 'demo',
                'checkpoint_root': 'dbfs:/checkpoints',
            },
            'storage': {
                'landing_root': 'abfss://landing',
                'bronze_root': 'abfss://bronze',
                'silver_root': 'abfss://silver',
                'gold_root': 'abfss://gold',
                'audit_root': 'abfss://audit',
            },
            'control_tables': {
                'catalog': 'main',
                'schema': 'control',
                'run_control_table': 'etl_run_control',
                'step_control_table': 'etl_step_control',
                'dq_results_table': 'etl_dq_results',
            },
            'runtime_defaults': {'is_full_load': False},
            'feature_flags': {},
            'secrets': {},
        }
    )


def _build_pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id='clients_curated',
        version=1,
        owner='data-eng',
        layer=Layer.SILVER,
        description='test pipeline',
        engine=Engine.PYSPARK,
        runtime_parameters=[
            RuntimeParameter(name='business_date', type=RuntimeParamType.DATE, required=True),
            RuntimeParameter(name='is_full_load', type=RuntimeParamType.BOOLEAN, required=False, default=False),
        ],
        sources=[
            SourceConfig(
                source_id='clients_src',
                source_type=SourceType.CSV,
                path='/tmp/clients.csv',
                options={'header': True},
            )
        ],
        targets=[
            TargetConfig(
                target_id='clients_silver',
                target_type=TargetType.DELTA,
                mode=WriteMode.OVERWRITE,
                catalog='main',
                schema='silver',
                table='clients_curated',
                path='/tmp/clients_silver',
            )
        ],
        steps=[
            ReadStepConfig(step_id='read_clients', step_type='read', source_id='clients_src', output_df='clients_raw'),
            TransformationStepConfig(
                step_id='dedup_clients',
                step_type='transformation',
                transformation='deduplicate',
                input_df='clients_raw',
                output_df='clients_curated_df',
                params={'key_columns': ['client_id']},
            ),
        ],
        dq=DQConfig(
            apply_to='clients_curated_df',
            include_rulepacks=['common_validations'],
            rules=[
                DQRuleConfig(
                    rule_id='clients_id_not_null',
                    rule_type='not_null',
                    severity=Severity.FAIL,
                    params={'columns': ['client_id']},
                )
            ],
        ),
        write=WriteConfig(input_df='clients_curated_df', target_id='clients_silver'),
    )


def test_validate_pipeline_semantics_accepts_valid_pipeline() -> None:
    pipeline = _build_pipeline()
    validate_pipeline_semantics(
        pipeline,
        available_rulepacks=['common_validations'],
        allowed_transformations=['deduplicate'],
    )


def test_validate_pipeline_semantics_rejects_unknown_input_df() -> None:
    pipeline = _build_pipeline()
    pipeline.steps[1].input_df = 'missing_df'

    try:
        validate_pipeline_semantics(
            pipeline,
            available_rulepacks=['common_validations'],
            allowed_transformations=['deduplicate'],
        )
    except ConfigValidationError as exc:
        assert "unknown input_df 'missing_df'" in str(exc)
    else:
        raise AssertionError('Expected ConfigValidationError for missing input_df')


def test_validate_runtime_against_pipeline_rejects_unexpected_runtime_param() -> None:
    pipeline = _build_pipeline()
    environment = _build_environment()
    runtime = RuntimeInvocation(
        pipeline_id='clients_curated',
        environment='dev',
        runtime={'business_date': '2026-03-26', 'unexpected_flag': True},
    )

    try:
        validate_runtime_against_pipeline(runtime, pipeline, environment)
    except ConfigValidationError as exc:
        assert 'unexpected runtime parameter' in str(exc)
    else:
        raise AssertionError('Expected ConfigValidationError for unexpected runtime parameter')
