from pathlib import Path

from pyspark.sql import DataFrame

from ea.util import get_active_spark_session
from sample_project.config import SourceConfig


def read_source(source_config: SourceConfig) -> DataFrame:
    return get_active_spark_session().read.format(source_config.format).load(str(source_config.path / source_config.name))


def write_and_read(df: DataFrame, dataset_name: str, build_location: Path, *, temporary: bool = False) -> DataFrame:
    """Write the DataFrame to a temporary location, store the query plan and read it back."""
    path = build_location / "temp" / dataset_name if temporary else build_location / dataset_name

    df.write.mode("overwrite").format("parquet").save(str(path))

    return get_active_spark_session().read.format("parquet").load(str(path))
