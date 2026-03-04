import logging
from pathlib import Path

from pyspark.sql import SparkSession

from data.utils import clean_data_directory
from ea.extract_execution_plan import ExtractExecutionPlan

logging.basicConfig(level=logging.WARNING)

module_path = Path(__file__).parent
build_path = module_path / "temp_build"
temp_path = build_path / "temp"
plan_path = build_path / "plans"
table_path = build_path / "tables"

clean_data_directory(temp_path, create_if_not_exists=True)
clean_data_directory(plan_path, create_if_not_exists=True)
clean_data_directory(table_path, create_if_not_exists=True)

# --- Test ---
spark = SparkSession.builder.appName("Testing").getOrCreate()

logging.getLogger("ea.extract_execution_plan").setLevel(logging.INFO)
with ExtractExecutionPlan(plan_path, temp_path, table_path, module_path):
    df = spark.range(1)
    df = df.cache()

    df.write.mode("overwrite").format("parquet").save(str(table_path / "test_table"))
    x = df.count()
    y = df.collect()

spark.stop()
