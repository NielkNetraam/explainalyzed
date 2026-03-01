import contextlib
import logging
from functools import partialmethod
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyspark.sql.dataframe as df_module
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from data.utils import get_active_spark_session, get_query_plan, store_plan
from sample_project.util import set_spark_conf_for_lineage

classes_to_patch = {"DataFrame": [df_module.DataFrame]}  # , "DataFrameWriter": [rw_module.DataFrameWriter]}

# Controleer of Spark Connect wordt gebruikt en pak die class ook mee
try:
    import pyspark.sql.connect.dataframe as connect_df_module

    classes_to_patch["DataFrame"].append(connect_df_module.DataFrame)
except ImportError:
    pass

try:
    import pyspark.sql.classic.dataframe as classic_df_module

    classes_to_patch["DataFrame"].append(classic_df_module.DataFrame)
except ImportError:
    pass


logger = logging.getLogger(__name__)


def wrap_dataframe_write(plan_path: Path, template_path: Path) -> None:
    """Patch DataFrameWriter methods to store query plans before writing data."""
    dataframe_writer_class = DataFrameWriter

    methods_to_wrap: dict[str, tuple[str, int]] = {
        "save": ("path", 0),
        "insertInto": ("tableName", 0),
        "saveAsTable": ("name", 0),
        "json": ("path", 0),
        "parquet": ("path", 0),
        "text": ("path", 0),
        "csv": ("path", 0),
        "xml": ("path", 0),
        "orc": ("path", 0),
        "jdbc": ("path", 0),
    }

    # Save the original methods
    original_methods = {
        method: getattr(dataframe_writer_class, method) for method in methods_to_wrap if hasattr(dataframe_writer_class, method)
    }

    def wrapper_func(self: DataFrameWriter, *args: Any, **kwargs: Any) -> None:
        """Wrap function to store the query plan before calling the original method."""
        method: str = kwargs["method"]
        method_to_wrap = methods_to_wrap[method]
        dataset_path = Path(
            args[method_to_wrap[1]] if len(args) > method_to_wrap[1] else kwargs.get(method_to_wrap[0], "unknown_dataset"),
        )

        with contextlib.suppress(ValueError):
            dataset_path = dataset_path.relative_to(template_path)

        dataset_name = str(dataset_path).replace("\\", "~").replace("/", "~")

        logger.info("Collect query plan on '%s' of '%s'", method, dataset_name)
        store_plan(get_query_plan(self._df), plan_path / f"{dataset_name}.txt")

        del kwargs["method"]
        original_methods[method](self, *args, **kwargs)

    # Wrap the methods
    for method in methods_to_wrap:
        setattr(dataframe_writer_class, method, partialmethod(wrapper_func, method=method))

    # add an unwrapped version of the original save method for internal use the dataframe wrapper
    dataframe_writer_class.unwrapped_save = original_methods["save"]  # type: ignore[attr-defined]


def wrap_dataframe(dataframe_class: type, temp_path: Path, plan_path: Path) -> None:
    methods_to_wrap: dict[str, str] = {
        "cache": "replace",
        "persist": "replace",
        "count": "original",
        "collect": "original",
        "take": "original",
        "tail": "original",
        "head": "original",
        "first": "original",
        "rdd": "original",
        "show": "original",
        # potential functions to add:
        # - "toJSON"
        # - "registerTempTable"
        # - "createTempView"
        # - "createOrReplaceTempView"
        # - "createGlobalTempView"
        # - "createOrReplaceGlobalTempView"
        # - "isEmpty"
        # - "checkpoint"
        # - "localCheckpoint"
        # - "toLocalIterator"
        # - "foreach"
        # - "foreachPartition"
        # -n"writeTo"
    }

    # Save the original methods
    original_methods = {
        method: getattr(dataframe_class, method) for method in methods_to_wrap if hasattr(dataframe_class, method)
    }

    def replace_wrapper_func(df: DataFrame, *args: Any, **kwargs: Any) -> DataFrame:
        method: str = kwargs["method"]
        dataset_name = f"{method}_{uuid4().hex[:8]}"

        logger.info("Collect query plan on '%s' ('%s')", method, dataset_name)
        store_plan(get_query_plan(df), plan_path / f"{dataset_name}.txt")

        logger.info("'%s' replaced by write and read of dataframe", method)
        df.write.mode("overwrite").format("parquet").unwrapped_save(str(temp_path / dataset_name))  # ty:ignore[unresolved-attribute]

        return get_active_spark_session().read.parquet(str(temp_path / dataset_name))

    def original_wrapper_func(df: DataFrame, *args: Any, **kwargs: Any) -> DataFrame:
        method: str = kwargs["method"]
        dataset_name = f"{method}_{uuid4().hex[:8]}"

        logger.info("Collect query plan on '%s' ('%s')", method, dataset_name)
        store_plan(get_query_plan(df), plan_path / f"{dataset_name}.txt")

        del kwargs["method"]
        return original_methods[method](df, *args, **kwargs)

    # Wrap the methods
    for method, wrap_type in methods_to_wrap.items():
        wrapper_func = replace_wrapper_func if wrap_type == "replace" else original_wrapper_func
        setattr(dataframe_class, method, partialmethod(wrapper_func, method=method))


# Patch alle mogelijke DataFrame classes
logging.basicConfig(level=logging.INFO)

build_path = Path(__file__).parent / "temp_build"
temp_path = build_path / "temp"
plan_path = build_path / "plans"

wrap_dataframe_write(plan_path, build_path)

for cls in classes_to_patch["DataFrame"]:
    wrap_dataframe(cls, temp_path, plan_path)

# --- Test ---
spark = SparkSession.builder.appName("Testing").getOrCreate()
set_spark_conf_for_lineage()
df = spark.range(1)
print(f"DataFrame type: {type(df)}")
df = df.cache()

df.write.mode("overwrite").format("parquet").save(str(build_path / "test_table"))
x = df.count()
y = df.collect()
spark.stop()
