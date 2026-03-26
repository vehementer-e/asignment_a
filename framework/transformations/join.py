from .base import BaseTransformation

class JoinTransformation(BaseTransformation):
    transformation_type = "join"

    def apply(self, df, step_config, context):
        params = self._params(step_config)
        right_df_name = params.get("right_df")
        join_type = params.get("join_type", "inner")
        on = params.get("on")
        if not right_df_name:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.right_df")
        if right_df_name not in context.datasets:
            raise ValueError(f"Join step references unknown dataset '{right_df_name}'")
        if not on:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.on")
        return df.join(context.datasets[right_df_name], on=on, how=join_type)
