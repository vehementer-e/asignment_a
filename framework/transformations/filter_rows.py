from .base import BaseTransformation

class FilterRowsTransformation(BaseTransformation):
    transformation_type = "filter_rows"

    def apply(self, df, step_config, context):
        condition = self._params(step_config).get("condition")
        if not condition:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.condition")
        return df.filter(condition)
