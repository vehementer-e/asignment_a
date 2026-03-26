from .base import BaseTransformation

class RenameColumnsTransformation(BaseTransformation):
    transformation_type = "rename_columns"

    def apply(self, df, step_config, context):
        mappings = self._params(step_config).get("mappings", {})
        if not mappings:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.mappings")
        out = df
        for old_name, new_name in mappings.items():
            out = out.withColumnRenamed(old_name, new_name)
        return out
