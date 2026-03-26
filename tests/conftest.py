from __future__ import annotations

import sys
from pathlib import Path

import pytest


def _add_candidate_paths() -> None:
    root = Path(__file__).resolve().parents[2]
    candidates = [
        root,
        root / 'gen_framework_io_transform',
        root / 'gen_framework_dq',
        root / 'gen_framework_audit',
        root / 'gen_framework_executor_integration',
    ]
    for path in candidates:
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))


_add_candidate_paths()


@pytest.fixture(scope='session')
def spark():
    pyspark = pytest.importorskip('pyspark')
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master('local[1]')
        .appName('config-driven-etl-tests')
        .getOrCreate()
    )
    yield spark
    spark.stop()
