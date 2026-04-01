import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

ID_PATTERN = r"(?:\blambda\s+[\w#]+)|(`[^`]+`#\d+|[\w]+(?:\([^()]*\))?#\d+[L]?)"


def get_dependencies(function_part: str) -> set[str]:
    src_fields = set(re.findall(ID_PATTERN, function_part))

    return {m.lower() for m in src_fields if m}


def split_field(field: str) -> tuple[str, set[str]]:
    if " AS " not in field:
        return field.lower(), {field.lower()}

    name_part = field.rsplit(" AS ", 1)[1].lower()
    function_part = field.rsplit(" AS ", 1)[0]

    if function_part in ("null", "NULL"):
        return name_part, {"__none__"}

    dependencies = list(get_dependencies(function_part))

    return name_part, set(dependencies) if len(dependencies) > 0 else {"__literal__"}


def split_fields(fields: list[str]) -> dict[str, list[str]]:
    splitted_fields: dict[str, list[str]] = {}

    for field in fields:
        name_part, dependencies = split_field(field)
        splitted_fields[name_part] = list(dependencies)

    return splitted_fields


def replace_within_parentheses(text: str, delimiter: str = ",", replacement: str = "§") -> str:
    depth = 0
    text_list = list(text)

    for i in range(len(text_list)):
        if text_list[i] in "({[":
            depth += 1
        elif text_list[i] in ")}]":
            depth -= 1

        if text_list[i] == delimiter and depth > 0:
            text_list[i] = replacement

    return "".join(text_list)


def strip_outer_parentheses(s: str) -> list[str]:
    return [f.replace("§", ",") for f in replace_within_parentheses(s).split(", ")]


def findall_column_ids(line: str) -> list[str]:
    return [cid.lower() for cid in set(re.findall(ID_PATTERN, line)) if cid]


def extract_derived_fields(fields: list[str]) -> dict[str, str]:
    return {
        name_part.lower(): function_part
        for function_part, name_part in (field.rsplit(" AS ", 1) for field in fields if " AS " in field)
    }


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
