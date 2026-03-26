from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class TransformationContext:
    spark: Any
    datasets: Dict[str, Any]
    runtime: Any | None = None
    environment: Any | None = None

class BaseTransformation(ABC):
    transformation_type: str = "base"

    @abstractmethod
    def apply(self, df: Any, step_config: Any, context: TransformationContext):
        raise NotImplementedError

    def _params(self, step_config: Any) -> Dict[str, Any]:
        return dict(getattr(step_config, "params", {}) or {})
