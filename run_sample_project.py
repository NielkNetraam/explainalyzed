import logging
from pathlib import Path

from pyspark.sql import SparkSession

from data.utils import clean_data_directory
from ea.explainanalyzed import ExplainAnalyzed
from ea.extract_execution_plan import ExtractExecutionPlan
from ea.lineage import Lineage
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

    plans = {p.stem: p for p in plan_path.glob("**/*.txt")}

    eas: dict[str, ExplainAnalyzed] = {}
    for name, plan in plans.items():
        with plan.open() as file:
            plan_data = file.readlines()

        eas[name] = ExplainAnalyzed(name, plan_data)

        path = run_path / "visualization" / f"{name}.mmd"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as file:
            file.write(eas[name].mermaid())

    path = run_path / "lineage/sample_project.mmd"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as file:
        file.write(Lineage.mermaid_from_lineages([ea.get_lineage() for ea in eas.values()]))

finally:
    spark.stop()
