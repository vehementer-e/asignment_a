"""Example wiring for the framework.dq package.

This file is illustrative and intentionally lightweight.
"""

from framework.dq import DataQualityExecutor, build_default_registry


def build_executor() -> DataQualityExecutor:
    return DataQualityExecutor(registry=build_default_registry())


if __name__ == '__main__':
    executor = build_executor()
    print(executor)
