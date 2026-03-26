from .base import BaseTransformation

class SqlTransformTransformation(BaseTransformation):
    transformation_type = "sql_transform"

    def apply(self, df, step_config, context):
        params = self._params(step_config)
        sql = params.get("sql")
        temp_view_name = params.get("temp_view_name", getattr(step_config, "input_df", "input_df"))
        if not sql:
            raise ValueError(f"Step '{getattr(step_config, 'step_id', '?')}' requires params.sql")
        df.createOrReplaceTempView(temp_view_name)
        for dataset_name, dataset_df in context.datasets.items():
            if dataset_name != temp_view_name:
                dataset_df.createOrReplaceTempView(dataset_name)
        return context.spark.sql(sql)
