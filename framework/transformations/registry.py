from .base import BaseTransformation
from .select_columns import SelectColumnsTransformation
from .rename_columns import RenameColumnsTransformation
from .cast_columns import CastColumnsTransformation
from .filter_rows import FilterRowsTransformation
from .join import JoinTransformation
from .derive_columns import DeriveColumnsTransformation
from .deduplicate import DeduplicateTransformation
from .sql_transform import SqlTransformTransformation

class TransformationRegistry:
    def __init__(self):
        self._transformations = {}

    @classmethod
    def with_defaults(cls) -> "TransformationRegistry":
        registry = cls()
        for transformation in (
            SelectColumnsTransformation(),
            RenameColumnsTransformation(),
            CastColumnsTransformation(),
            FilterRowsTransformation(),
            JoinTransformation(),
            DeriveColumnsTransformation(),
            DeduplicateTransformation(),
            SqlTransformTransformation(),
        ):
            registry.register(transformation)
        return registry

    def register(self, transformation: BaseTransformation) -> None:
        self._transformations[transformation.transformation_type.lower()] = transformation

    def get(self, transformation_type: str) -> BaseTransformation:
        key = (transformation_type or "").lower()
        if key not in self._transformations:
            raise ValueError(f"Unsupported transformation type: {transformation_type}")
        return self._transformations[key]

    def apply(self, df, step_config, context):
        return self.get(getattr(step_config, "type", None)).apply(df, step_config, context)
