from pathlib import Path
from shutil import rmtree

from pyspark.sql import DataFrame, SparkSession


def clean_data_directory(path: Path) -> None:
    if path.exists():
        print(f"Cleaning up existing data at {path}")
        rmtree(str(path))


def store_dataframe(df: DataFrame, path: Path) -> None:
    df.write.mode("overwrite").save(str(path))


def store_plan(plan: str, path: Path) -> None:
    with path.open("w") as file:
        file.write(plan)


def get_query_plan(df: DataFrame) -> str:
    """Get the formatted query plan of a DataFrame as a string.

    Alternative way:
        df._sc._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    """
    return df._jdf.queryExecution().toString()  # noqa: SLF001


def write_and_read(df: DataFrame, data_location: Path, plan_location: Path, dataset_name: str) -> DataFrame:
    """Write the DataFrame to a temporary location, store the query plan and read it back."""
    plan = get_query_plan(df)
    store_plan(plan, plan_location / f"{dataset_name}.txt")

    df.write.mode("overwrite").format("parquet").save(str(data_location / dataset_name))

    spark = SparkSession.getActiveSession()

    if spark is None:
        msg = "No active SparkSession found."
        raise RuntimeError(msg)

    return spark.read.parquet(str(data_location / dataset_name))
