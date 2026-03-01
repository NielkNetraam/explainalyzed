from pyspark.sql import DataFrame

from data.utils import get_active_spark_session, get_query_plan, store_plan
from sample_project.config import Config, SourceConfig


def read_source(source_config: SourceConfig) -> DataFrame:
    return get_active_spark_session().read.format(source_config.format).load(str(source_config.path / source_config.name))


def write_and_read(df: DataFrame, dataset_name: str, config: Config, *, temporary: bool = False) -> DataFrame:
    """Write the DataFrame to a temporary location, store the query plan and read it back."""
    if config.lineage and config.plan_location is not None:
        plan = get_query_plan(df)

        store_plan(plan, config.plan_location / f"{dataset_name}.txt")

    path = config.build_location / "temp" / dataset_name if temporary else config.build_location / dataset_name

    df.write.mode("overwrite").format("parquet").save(str(path))

    return get_active_spark_session().read.format("parquet").load(str(path))


def set_spark_conf_for_lineage() -> None:
    spark = get_active_spark_session()
    spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
    spark.conf.set("spark.sql.maxMetadataStringLength", "10000")
