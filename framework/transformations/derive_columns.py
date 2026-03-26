import re
from pyspark.sql import functions as F
from .base import BaseTransformation

_RUNTIME_PATTERN = re.compile(r"^\{\{\s*runtime\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}$")

class DeriveColumnsTransformation(BaseTransformation):
    transformation_type = "derive_columns"

    def apply(self, df, step_config, context):
        columns = self._params(step_config).get("columns", [])
        if not columns:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.columns")
        out = df
        for spec in columns:
            name = spec["name"]
            if "sql_expr" in spec:
                out = out.withColumn(name, F.expr(spec["sql_expr"]))
            elif "literal" in spec:
                value = spec["literal"]
                if isinstance(value, str):
                    m = _RUNTIME_PATTERN.match(value)
                    if m and context.runtime is not None:
                        key = m.group(1)
                        value = context.runtime.get(key) if isinstance(context.runtime, dict) else getattr(context.runtime, key, None)
                out = out.withColumn(name, F.lit(value))
            else:
                raise ValueError(f"Derived column '{name}' must define sql_expr or literal")
        return out
