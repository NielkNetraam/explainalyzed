from pathlib import Path
from shutil import rmtree

from pyspark.sql import DataFrame


def clean_data_directory(path: Path) -> None:
    if path.exists():
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
