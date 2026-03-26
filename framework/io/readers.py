from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class ReaderContext:
    spark: Any
    environment: Any | None = None
    runtime: Any | None = None

class BaseReader(ABC):
    format_name: str = "base"

    @abstractmethod
    def read(self, source_config: Any, context: ReaderContext):
        raise NotImplementedError

class CsvReader(BaseReader):
    format_name = "csv"

    def read(self, source_config: Any, context: ReaderContext):
        options = dict(getattr(source_config, "options", {}) or {})
        header = options.pop("header", True)
        infer_schema = options.pop("inferSchema", True)
        return (
            context.spark.read.format("csv")
            .options(**options)
            .option("header", header)
            .option("inferSchema", infer_schema)
            .load(source_config.path)
        )

class JsonReader(BaseReader):
    format_name = "json"

    def read(self, source_config: Any, context: ReaderContext):
        options = dict(getattr(source_config, "options", {}) or {})
        multiline = options.pop("multiLine", True)
        return (
            context.spark.read.format("json")
            .options(**options)
            .option("multiLine", multiline)
            .load(source_config.path)
        )

class ParquetReader(BaseReader):
    format_name = "parquet"

    def read(self, source_config: Any, context: ReaderContext):
        options = dict(getattr(source_config, "options", {}) or {})
        return context.spark.read.format("parquet").options(**options).load(source_config.path)

class DeltaReader(BaseReader):
    format_name = "delta"

    def read(self, source_config: Any, context: ReaderContext):
        options = dict(getattr(source_config, "options", {}) or {})
        return context.spark.read.format("delta").options(**options).load(source_config.path)

class ReaderRegistry:
    def __init__(self):
        self._readers: Dict[str, BaseReader] = {}

    @classmethod
    def with_defaults(cls) -> "ReaderRegistry":
        registry = cls()
        for reader in (CsvReader(), JsonReader(), ParquetReader(), DeltaReader()):
            registry.register(reader)
        return registry

    def register(self, reader: BaseReader) -> None:
        self._readers[reader.format_name.lower()] = reader

    def get(self, format_name: str) -> BaseReader:
        key = (format_name or "").lower()
        if key not in self._readers:
            raise ValueError(f"Unsupported reader format: {format_name}")
        return self._readers[key]

    def read(self, source_config: Any, context: ReaderContext):
        return self.get(getattr(source_config, "format", None)).read(source_config, context)
