from pathlib import Path
from shutil import rmtree

from pyspark.sql import DataFrame

from ea.util import get_active_spark_session, get_query_plan, store_plan


def clean_data_directory(path: Path, *, create_if_not_exists: bool = False) -> None:
    if path.exists():
        print(f"Cleaning up existing data at {path}")  # noqa: T201
        rmtree(str(path))
        path.mkdir(parents=True, exist_ok=True)
    elif create_if_not_exists:
        path.mkdir(parents=True, exist_ok=True)


def store_dataframe(df: DataFrame, path: Path) -> None:
    df.write.mode("overwrite").save(str(path))


def store_json(df: DataFrame, path: Path) -> None:
    df.coalesce(1).write.mode("overwrite").json(str(path))


def write_and_read(df: DataFrame, data_location: Path, plan_location: Path, dataset_name: str) -> DataFrame:
    """Write the DataFrame to a temporary location, store the query plan and read it back."""
    plan = get_query_plan(df)
    store_plan(plan, plan_location / f"{dataset_name}.txt")

    df.write.mode("overwrite").format("parquet").save(str(data_location / dataset_name))

    return get_active_spark_session().read.parquet(str(data_location / dataset_name))
