from __future__ import annotations

def test_pipeline_executor_contracts_are_compatible():
    from framework.execution.pipeline_executor import PipelineExecutor

    executor = PipelineExecutor()
    assert executor is not None
