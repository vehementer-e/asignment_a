from pyspark.sql import Window
from pyspark.sql import functions as F
from .base import BaseTransformation

class DeduplicateTransformation(BaseTransformation):
    transformation_type = "deduplicate"

    def apply(self, df, step_config, context):
        params = self._params(step_config)
        keys = params.get("keys", [])
        order_by = params.get("order_by", [])
        keep = params.get("keep", "first").lower()
        if not keys:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.keys")
        if keep not in {"first", "last"}:
            raise ValueError("params.keep must be 'first' or 'last'")
        sort_exprs = []
        for item in order_by:
            col_name = item["column"]
            direction = item.get("direction", "desc").lower()
            expr = F.col(col_name).asc() if direction == "asc" else F.col(col_name).desc()
            if keep == "last":
                expr = F.col(col_name).desc() if direction == "asc" else F.col(col_name).asc()
            sort_exprs.append(expr)
        if not sort_exprs:
            sort_exprs = [F.lit(1)]
        w = Window.partitionBy(*keys).orderBy(*sort_exprs)
        return (
            df.withColumn("_etl_row_number", F.row_number().over(w))
              .filter(F.col("_etl_row_number") == 1)
              .drop("_etl_row_number")
        )
