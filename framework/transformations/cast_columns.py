from pyspark.sql import functions as F
from .base import BaseTransformation

class CastColumnsTransformation(BaseTransformation):
    transformation_type = "cast_columns"

    def apply(self, df, step_config, context):
        casts = self._params(step_config).get("casts", {})
        if not casts:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.casts")
        out = df
        for column_name, target_type in casts.items():
            out = out.withColumn(column_name, F.col(column_name).cast(target_type))
        return out
