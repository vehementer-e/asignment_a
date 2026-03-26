from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class DQStatus(str, Enum):
    PASSED = 'PASSED'
    FAILED = 'FAILED'
    WARNING = 'WARNING'
    SKIPPED = 'SKIPPED'
    ERROR = 'ERROR'


class DQResult(BaseModel):
    rule_id: str
    rule_type: str
    dataset: Optional[str] = None
    column: Optional[str] = None
    severity: str = 'warn'
    status: DQStatus
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    threshold: Optional[Any] = None
    failed_count: Optional[int] = None
    row_count: Optional[int] = None
    message: str = ''
    details: Dict[str, Any] = Field(default_factory=dict)

    @property
    def is_failure(self) -> bool:
        return self.status in {DQStatus.FAILED, DQStatus.ERROR}


class DQExecutionSummary(BaseModel):
    pipeline_id: Optional[str] = None
    run_id: Optional[str] = None
    dataset: Optional[str] = None
    overall_status: DQStatus = DQStatus.PASSED
    result_count: int = 0
    passed_count: int = 0
    failed_count: int = 0
    warning_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    results: List[DQResult] = Field(default_factory=list)

    @classmethod
    def from_results(
        cls,
        results: List[DQResult],
        pipeline_id: Optional[str] = None,
        run_id: Optional[str] = None,
        dataset: Optional[str] = None,
    ) -> 'DQExecutionSummary':
        summary = cls(
            pipeline_id=pipeline_id,
            run_id=run_id,
            dataset=dataset,
            result_count=len(results),
            results=results,
        )
        for result in results:
            if result.status == DQStatus.PASSED:
                summary.passed_count += 1
            elif result.status == DQStatus.FAILED:
                summary.failed_count += 1
            elif result.status == DQStatus.WARNING:
                summary.warning_count += 1
            elif result.status == DQStatus.SKIPPED:
                summary.skipped_count += 1
            elif result.status == DQStatus.ERROR:
                summary.error_count += 1

        if summary.error_count > 0:
            summary.overall_status = DQStatus.ERROR
        elif summary.failed_count > 0:
            summary.overall_status = DQStatus.FAILED
        elif summary.warning_count > 0:
            summary.overall_status = DQStatus.WARNING
        else:
            summary.overall_status = DQStatus.PASSED
        return summary
