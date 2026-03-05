import logging
from pathlib import Path

from pyspark.sql import SparkSession

from data.utils import clean_data_directory
from ea.explainanalyzed import ExplainAnalyzed
from ea.extract_execution_plan import ExtractExecutionPlan
from sample_project.builder import builder
from sample_project.config import SourceConfig

logging.basicConfig(level=logging.WARNING)
logging.getLogger("ea.extract_execution_plan").setLevel(logging.INFO)

project_path = Path(__file__).parent
run_path = project_path / "sample_project" / "run"

build_path = run_path / "build"
temp_path = run_path / "build" / "temp"
plan_path = run_path / "plans"
visualization_path = run_path / "visualization"
lineage_path = run_path / "lineage"

source_config: dict[str, SourceConfig] = {
    "sample_table": SourceConfig(
        name="sample_table",
        path=project_path / "data" / "tables",
    ),
    "sample_table_2": SourceConfig(
        name="sample_table_2",
        path=project_path / "data" / "tables",
    ),
    "relation_table": SourceConfig(
        name="relation_table",
        path=project_path / "data" / "tables",
    ),
}

spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()

clean_data_directory(build_path, create_if_not_exists=True)
clean_data_directory(plan_path, create_if_not_exists=True)
clean_data_directory(temp_path, create_if_not_exists=True)
clean_data_directory(visualization_path, create_if_not_exists=True)
clean_data_directory(lineage_path, create_if_not_exists=True)

try:
    with ExtractExecutionPlan(
        plan_path=plan_path,
        temp_path=temp_path,
        build_path=build_path,
        module_path=Path(__file__).parent,
    ):
        builder(build_path, source_config)

    ExplainAnalyzed.from_path(plan_path, run_path, "sample_project", plan_visualisation=True, intermediate_lineage=True)

finally:
    spark.stop()
