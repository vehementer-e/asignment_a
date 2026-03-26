from .base import BaseTransformation

class SelectColumnsTransformation(BaseTransformation):
    transformation_type = "select_columns"

    def apply(self, df, step_config, context):
        columns = self._params(step_config).get("columns", [])
        if not columns:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.columns")
        return df.select(*columns)
