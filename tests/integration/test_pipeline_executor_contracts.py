from __future__ import annotations

import pytest


@pytest.mark.xfail(
    reason=(
        'Current generated layers still have contract mismatches: '\
        'executor expects ReaderRegistry.with_defaults/WriterRegistry.with_defaults/TransformationRegistry.with_defaults, '\
        'uses step.type instead of step.step_type in places, and audit metadata signatures still differ.'
    ),
    strict=False,
)
def test_pipeline_executor_current_contract_mismatches_are_visible():
    from framework.execution.pipeline_executor import PipelineExecutor

    # The constructor already touches some of the mismatched contracts.
    PipelineExecutor()
