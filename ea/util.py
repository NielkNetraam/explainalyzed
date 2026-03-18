import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

ID_PATTERN = r"(?<!lambda )\b(\w[\w\d\_\-]*\#\d*[L]?)"


def replace_within_parentheses(text: str, delimiter: str = ",", replacement: str = "§") -> str:
    depth = 0
    text_list = list(text)

    for i in range(len(text_list)):
        if text_list[i] in "({[]":
            depth += 1
        elif text_list[i] in ")}]":
            depth -= 1

        if text_list[i] == delimiter and depth > 0:
            text_list[i] = replacement

    return "".join(text_list)


def strip_outer_parentheses(s: str) -> list[str]:
    return [f.replace("§", ",") for f in replace_within_parentheses(s).split(", ")]


def findall_column_ids(line: str) -> list[str]:
    return list(set(re.findall(ID_PATTERN, line)))


def get_active_spark_session() -> SparkSession:
    """Get the active SparkSession or raise an error if none is found."""
    spark = SparkSession.getActiveSession()

    if spark is None:
        msg = "No active SparkSession found."
        raise RuntimeError(msg)

    return spark


def store_plan(plan: str, path: Path) -> None:
    with path.open("w") as file:
        file.write(plan)


def get_query_plan(df: DataFrame) -> str:
    """Get the formatted query plan of a DataFrame as a string.

    Alternative way:
        df._sc._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    """
    plan = df._jdf.queryExecution().toString()  # noqa: SLF001

    if "..." in plan:
        raise IncompleteExecutionPlanError

    return plan


class IncompleteExecutionPlanError(Exception):
    def __init__(self) -> None:
        msg = (
            "execution plan contains '...', increase 'spark.sql.debug.maxToStringFields' and/or "
            "'spark.sql.maxMetadataStringLength' when extracting execution plans"
        )
        super().__init__(msg)
