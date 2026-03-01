from pathlib import Path

from pyspark.sql import SparkSession

from ea.explainanalyzed import ExplainAnalyzed
from ea.lineage import Lineage
from sample_project.builder import builder
from sample_project.config import Config, SourceConfig

project_path = Path(__file__).parent
run_path = project_path / "sample_project" / "run"

config = Config(
    build_location=run_path / "build",
    lineage=True,
    plan_location=run_path / "plans",
)

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

try:
    builder(config, source_config)

    if config.lineage and config.plan_location is not None:
        plans = {p.stem: p for p in config.plan_location.glob("**/*.txt")}

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
