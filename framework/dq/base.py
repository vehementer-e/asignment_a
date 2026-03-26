from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from .models import DQResult


class DataQualityRule(ABC):
    rule_type: str = 'base'

    @abstractmethod
    def evaluate(
        self,
        df: Any,
        rule_config: Any,
        context: Optional[Dict[str, Any]] = None,
    ) -> DQResult:
        raise NotImplementedError
