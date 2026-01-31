from pyspark.sql import SparkSession

from data.create_data import create_plans_and_store, create_tables_and_store


def main():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    if False:
        create_tables_and_store(spark)
    create_plans_and_store(spark)

    spark.stop()


if __name__ == "__main__":
    main()
