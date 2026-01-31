from types import GeneratorType

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> GeneratorType[SparkSession]:
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

    spark.stop()
