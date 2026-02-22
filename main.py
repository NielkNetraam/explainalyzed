from pyspark.sql import SparkSession

from data.create_data import create_plans_and_store, create_tables_and_store
from data.create_sample import create_sample_plans


def main() -> None:
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    if False:
        create_tables_and_store(spark)
    create_plans_and_store(spark)
    create_sample_plans(spark)

    spark.stop()


if __name__ == "__main__":
    main()
