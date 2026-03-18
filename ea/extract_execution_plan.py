import contextlib
import inspect
import logging
from collections.abc import Callable
from functools import partialmethod
from pathlib import Path
from typing import Any

import pyspark.sql.dataframe as df_module
from pyspark.sql import DataFrame, DataFrameWriter

from ea.util import get_active_spark_session, get_query_plan, store_plan

classes_to_patch = {"DataFrame": [df_module.DataFrame]}

# try to import DataFrame classes for different Spark versions and add them to the list of classes to patch
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


class ExtractExecutionPlan:
    def __init__(
        self,
        plan_path: Path,
        temp_path: Path,
        build_path: Path | None = None,
        module_path: Path | None = None,
    ) -> None:
        self.plan_path = plan_path
        self.temp_path = temp_path
        self.build_path = build_path
        self.module_path = module_path
        self.original_write_methods: dict[str, Callable] = {}
        self.spark = get_active_spark_session()

        self.spark_conf: dict[str, int] = {
            "spark.sql.debug.maxToStringFields": 1000,
            "spark.sql.maxMetadataStringLength": 100000,
        }
        self.original_spark_conf: dict[str, int] = {}
        self.original_dataframe_methods: dict[type, dict[str, Callable]] = {}
        self._wrapped = False
        self.unique_names: dict[str, int] = {}

    def _get_unique_name(self, name: str) -> str:
        if name in self.unique_names:
            self.unique_names[name] += 1
        else:
            self.unique_names[name] = 1

        return f"{name}_{self.unique_names[name]}"

    def _wrap_dataframe_write(self) -> dict[str, Callable]:
        """Patch DataFrameWriter methods to store query plans before writing data."""
        dataframe_writer_class = DataFrameWriter

        logger.info("Wrapping methods of '%s' in '%s", dataframe_writer_class.__name__, dataframe_writer_class.__module__)

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

        temp_path = self.temp_path
        plan_path = self.plan_path
        build_path = self.build_path

        # Save the original methods
        original_write_methods: dict[str, Callable] = {
            method: getattr(dataframe_writer_class, method)
            for method in methods_to_wrap
            if hasattr(dataframe_writer_class, method)
        }

        def wrapper_func(self: DataFrameWriter, *args: Any, **kwargs: Any) -> None:
            """Wrap function to store the query plan before calling the original method."""
            method_name: str = kwargs["method"]
            original_method = original_write_methods[method_name]
            method_to_wrap = methods_to_wrap[method_name]

            dataset_path = Path(
                args[method_to_wrap[1]] if len(args) > method_to_wrap[1] else kwargs.get(method_to_wrap[0], "unknown_dataset"),
            )

            with contextlib.suppress(ValueError):
                if temp_path:
                    dataset_path = dataset_path.relative_to(temp_path)

            with contextlib.suppress(ValueError):
                if build_path:
                    dataset_path = dataset_path.relative_to(build_path)

            dataset_name = str(dataset_path).replace("\\", "~").replace("/", "~")

            logger.info("Collect query plan on '%s' of '%s'", method_name, dataset_name)
            store_plan(get_query_plan(self._df), plan_path / f"{dataset_name}.txt")

            del kwargs["method"]
            original_method(self, *args, **kwargs)

        # Wrap the methods
        for method in methods_to_wrap:
            logger.debug("Wrapping '%s' of '%s'", method, dataframe_writer_class.__name__)
            setattr(
                dataframe_writer_class,
                method,
                partialmethod(wrapper_func, method=method),
            )

        # add an unwrapped version of the original save method for internal use the dataframe wrapper
        dataframe_writer_class.unwrapped_save = original_write_methods["save"]  # type: ignore[attr-defined]

        return original_write_methods

    def _wrap_dataframe(self, dataframe_class: type, temp_path: Path, plan_path: Path) -> dict[str, Callable]:
        logger.info("Wrapping methods of '%s' in '%s", dataframe_class.__name__, dataframe_class.__module__)

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
            "isEmpty": "original",
            # potential functions to add:
            # - "toJSON"
            # - "registerTempTable"
            # - "createTempView"
            # - "createOrReplaceTempView"
            # - "createGlobalTempView"
            # - "createOrReplaceGlobalTempView"
            # - "checkpoint"
            # - "localCheckpoint"
            # - "toLocalIterator"
            # - "foreach"
            # - "foreachPartition"
            # - "writeTo"
        }

        module_path = self.module_path

        # Save the original methods
        original_methods: dict[str, Callable] = {
            method: getattr(dataframe_class, method) for method in methods_to_wrap if hasattr(dataframe_class, method)
        }

        def replace_wrapper_func(df: DataFrame, *args: Any, **kwargs: Any) -> DataFrame:  # noqa: ARG001
            frame = inspect.stack()[1]

            file_name = Path(frame.filename)
            line_number = frame.lineno

            method: str = kwargs["method"]

            logger.info("Collect query plan on '%s' called from %s:%s", method, file_name, line_number)

            with contextlib.suppress(ValueError):
                if module_path:
                    file_name = file_name.relative_to(module_path)

            file_name = str(file_name).replace("\\", "~").replace("/", "~").replace(".", "_")

            dataset_name = self._get_unique_name(f"{method}_{file_name}_{line_number}")

            store_plan(get_query_plan(df), plan_path / f"{dataset_name}.txt")

            logger.info("Replaced '%s' by write and read of dataframe", method)
            df.write.mode("overwrite").format("parquet").unwrapped_save(str(temp_path / dataset_name))  # ty:ignore[unresolved-attribute]

            return get_active_spark_session().read.parquet(str(temp_path / dataset_name))

        def original_wrapper_func(df: DataFrame, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            frame = inspect.stack()[1]

            file_name = Path(frame.filename)
            line_number = frame.lineno

            method: str = kwargs["method"]

            logger.info("Collect query plan on '%s' called from %s:%s", method, file_name, line_number)

            with contextlib.suppress(ValueError):
                if module_path:
                    file_name = file_name.relative_to(module_path)

            file_name = str(file_name).replace("\\", "~").replace("/", "~").replace(".", "_")

            dataset_name = self._get_unique_name(f"{method}_{file_name}_{line_number}")
            store_plan(get_query_plan(df), plan_path / f"{dataset_name}.txt")

            del kwargs["method"]
            return original_methods[method](df, *args, **kwargs)

        # Wrap the methods
        for method, wrap_type in methods_to_wrap.items():
            logger.debug("Wrapping '%s' of '%s' with wrap type '%s'", method, dataframe_class.__name__, wrap_type)
            wrapper_func = replace_wrapper_func if wrap_type == "replace" else original_wrapper_func
            setattr(dataframe_class, method, partialmethod(wrapper_func, method=method))

        return original_methods

    def wrap_dataframe(self) -> None:
        logger.info("Wrapping DataFrame methods to extract execution plans.")
        self._set_spark_conf(self.spark_conf, highest=True)

        if self._wrapped:
            logger.warning("DataFrame methods are already wrapped. Skipping wrapping again.")
            return

        self.original_write_methods = self._wrap_dataframe_write()
        self.original_dataframe_methods = {}

        for cls in classes_to_patch["DataFrame"]:
            self.original_dataframe_methods[cls] = self._wrap_dataframe(cls, self.temp_path, self.plan_path)

        self._wrapped = True

    def unwrap_dataframe(self) -> None:
        # Restore original methods for DataFrameWriter
        dataframe_writer_class = DataFrameWriter
        for method, original_method in self.original_write_methods.items():
            setattr(dataframe_writer_class, method, original_method)
        delattr(dataframe_writer_class, "unwrapped_save")

        # Restore original methods for DataFrame classes
        for dataframe_class, original_methods in self.original_dataframe_methods.items():
            for method, original_method in original_methods.items():
                setattr(dataframe_class, method, original_method)

        self._set_spark_conf(self.original_spark_conf, highest=False)

        self.original_write_methods = {}
        self.original_dataframe_methods = {}
        self._wrapped = False

        logger.info("Original DataFrame methods have been restored.")

    def _set_spark_conf(self, spark_conf: dict[str, int], *, highest: bool = True) -> None:
        self.original_spark_conf = {key: int(self.spark.conf.get(key)) for key in self.spark_conf}

        for key, value in spark_conf.items():
            value_to_set = value if not highest else max(value, self.spark_conf.get(key, value))
            logger.debug("set '%s' to '%s'", key, value_to_set)
            self.spark.conf.set(key, str(value_to_set))

    def __enter__(self) -> None:
        """Context manager to wrap DataFrame methods for storing execution plans."""
        self.wrap_dataframe()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Restore original DataFrame methods."""
        self.unwrap_dataframe()
