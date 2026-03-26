from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class WriterContext:
    spark: Any
    environment: Any | None = None
    runtime: Any | None = None

class BaseWriter(ABC):
    format_name: str = "base"

    @abstractmethod
    def write(self, df: Any, target_config: Any, context: WriterContext) -> Dict[str, Any]:
        raise NotImplementedError

class DeltaWriter(BaseWriter):
    format_name = "delta"

    def write(self, df: Any, target_config: Any, context: WriterContext) -> Dict[str, Any]:
        options = dict(getattr(target_config, "options", {}) or {})
        mode = getattr(target_config, "mode", "overwrite")
        path = getattr(target_config, "path")
        partition_by = list(getattr(target_config, "partition_by", None) or [])
        writer = df.write.format("delta").mode(mode).options(**options)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return {"target_id": getattr(target_config, "target_id", None), "format": "delta", "path": path, "mode": mode}

class ParquetWriter(BaseWriter):
    format_name = "parquet"

    def write(self, df: Any, target_config: Any, context: WriterContext) -> Dict[str, Any]:
        options = dict(getattr(target_config, "options", {}) or {})
        mode = getattr(target_config, "mode", "overwrite")
        path = getattr(target_config, "path")
        partition_by = list(getattr(target_config, "partition_by", None) or [])
        writer = df.write.format("parquet").mode(mode).options(**options)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return {"target_id": getattr(target_config, "target_id", None), "format": "parquet", "path": path, "mode": mode}

class ConsoleWriter(BaseWriter):
    format_name = "console"

    def write(self, df: Any, target_config: Any, context: WriterContext) -> Dict[str, Any]:
        options = dict(getattr(target_config, "options", {}) or {})
        show_rows = int(options.get("show_rows", 20))
        truncate = bool(options.get("truncate", False))
        df.show(show_rows, truncate=truncate)
        return {"target_id": getattr(target_config, "target_id", None), "format": "console", "rows_shown": show_rows}

class WriterRegistry:
    def __init__(self):
        self._writers: Dict[str, BaseWriter] = {}
        for writer in (DeltaWriter(), ParquetWriter(), ConsoleWriter()):
            self.register(writer)

    def register(self, writer: BaseWriter) -> None:
        self._writers[writer.format_name.lower()] = writer

    def get(self, format_name: str) -> BaseWriter:
        key = (format_name or "").lower()
        if key not in self._writers:
            raise ValueError(f"Unsupported writer format: {format_name}")
        return self._writers[key]

    def write(self, df: Any, target_config: Any, context: WriterContext) -> Dict[str, Any]:
        return self.get(getattr(target_config, "format", None)).write(df, target_config, context)
