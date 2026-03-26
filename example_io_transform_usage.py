from framework.io.readers import ReaderRegistry, ReaderContext
from framework.io.writers import WriterRegistry, WriterContext
from framework.transformations.registry import TransformationRegistry
from framework.transformations.base import TransformationContext

def example(reader_source_cfg, target_cfg, step_cfg, spark):
    readers = ReaderRegistry()
    writers = WriterRegistry()
    transformations = TransformationRegistry()

    df = readers.read(reader_source_cfg, ReaderContext(spark=spark))
    datasets = {getattr(step_cfg, "input_df", "input_df"): df}
    transformed = transformations.apply(
        df,
        step_cfg,
        TransformationContext(
            spark=spark,
            datasets=datasets,
            runtime={"business_date": "2026-03-26"},
        ),
    )
    writers.write(transformed, target_cfg, WriterContext(spark=spark))
