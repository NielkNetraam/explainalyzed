from pathlib import Path

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType

from data.utils import clean_data_directory, write_and_read

SAMPLE_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
    ],
)
SAMPLE_SCHEMA_2 = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("birth_date", DateType(), nullable=True),
    ],
)
RELATION_SCHEMA = StructType(
    [
        StructField("id1", IntegerType(), nullable=False),
        StructField("id2", IntegerType(), nullable=True),
        StructField("rel_type", StringType(), nullable=True),
    ],
)
TRX_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("amount", DecimalType(10, 2), nullable=True),
        StructField("sign", StringType(), nullable=True),
    ],
)


TABLE_PATH = Path(__file__).parent / "tables"
SAMPLE_TABLE_PATH = Path(__file__).parent / "sample" / "tables"
SAMPLE_PLAN_PATH = Path(__file__).parent / "sample" / "plans"


def prep_sample_table(spark: SparkSession) -> DataFrame:
    return (
        spark.read.load(str(TABLE_PATH / "sample_table"))
        .filter(f.col("id") > 1)
        .withColumn("name_upper", f.upper(f.col("name")))
        .select("id", "name_upper")
    )


def prep_sample_table_2(spark: SparkSession) -> DataFrame:
    return (
        spark.read.load(str(TABLE_PATH / "sample_table_2"))
        .filter(f.col("id") < 10)  # noqa: PLR2004
        .withColumn("name_upper", f.upper(f.col("name")))
        .select("id", "name_upper")
    )


def prep_relation_table(spark: SparkSession) -> DataFrame:
    return (
        spark.read.load(str(TABLE_PATH / "relation_table"))
        .filter(f.col("id1") > 1)
        .filter(f.col("id2") < 10)  # noqa: PLR2004
        .filter(f.col("rel_type") == "friend")
    )


def create_complex_example_plans(spark: SparkSession) -> None:
    spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
    spark.conf.set("spark.sql.maxMetadataStringLength", "10000")

    clean_data_directory(SAMPLE_PLAN_PATH)
    SAMPLE_PLAN_PATH.mkdir(parents=True, exist_ok=True)
    clean_data_directory(SAMPLE_TABLE_PATH)
    SAMPLE_TABLE_PATH.mkdir(parents=True, exist_ok=True)

    df1 = prep_sample_table(spark)
    df2 = prep_sample_table_2(spark)
    df_rel = prep_relation_table(spark)

    df1 = write_and_read(df1, SAMPLE_TABLE_PATH, SAMPLE_PLAN_PATH, "sample_table_prepped")
    df2 = write_and_read(df2, SAMPLE_TABLE_PATH, SAMPLE_PLAN_PATH, "sample_table_2_prepped")
    df_rel = write_and_read(df_rel, SAMPLE_TABLE_PATH, SAMPLE_PLAN_PATH, "relation_table_prepped")

    df = df1.unionByName(df2, allowMissingColumns=True)

    df_joined = (
        df.alias("df_a")
        .join(df_rel.filter(f.col("rel_type") == "friend"), f.col("df_a.id") == df_rel.id1, how="inner")
        .join(df.alias("df_b"), f.col("df_b.id") == df_rel.id2, how="inner")
        .select(f.col("df_a.name_upper").alias("name_a"), f.col("df_b.name_upper").alias("name_b"))
    )

    df_joined = write_and_read(df_joined, SAMPLE_TABLE_PATH, SAMPLE_PLAN_PATH, "df_joined")
