from __future__ import annotations

from typing import Dict, Type

from .base import DataQualityRule


class DQRuleRegistry:
    def __init__(self) -> None:
        self._rules: Dict[str, Type[DataQualityRule]] = {}

    def register(self, rule_type: str, rule_cls: Type[DataQualityRule]) -> None:
        self._rules[rule_type] = rule_cls

    def has(self, rule_type: str) -> bool:
        return rule_type in self._rules

    def get(self, rule_type: str) -> Type[DataQualityRule]:
        if rule_type not in self._rules:
            available = ', '.join(sorted(self._rules.keys()))
            raise KeyError(f"Unknown DQ rule type '{rule_type}'. Available: {available}")
        return self._rules[rule_type]

    def create(self, rule_type: str) -> DataQualityRule:
        return self.get(rule_type)()

    @classmethod
    def with_defaults(cls) -> "DQRuleRegistry":
        from .accepted_values import AcceptedValuesRule
        from .not_null import NotNullRule
        from .numeric_range import NumericRangeRule
        from .uniqueness import UniquenessRule

        registry = cls()
        registry.register('not_null', NotNullRule)
        registry.register('uniqueness', UniquenessRule)
        registry.register('accepted_values', AcceptedValuesRule)
        registry.register('numeric_range', NumericRangeRule)
        return registry

def build_default_registry() -> DQRuleRegistry:
    return DQRuleRegistry.with_defaults()
