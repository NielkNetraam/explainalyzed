from datetime import date
from decimal import Decimal
from pathlib import Path

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType

from data.utils import clean_data_directory, get_query_plan, store_dataframe, store_plan

SAMPLE_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
    ],
)
SAMPLE_DATA: list[tuple[int, str, int]] = [
    (1, "Alice", 30),
    (2, "Bob", 25),
    (3, "Charlie", 35),
]
SAMPLE_SCHEMA_2 = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("birth_date", DateType(), nullable=True),
    ],
)
SAMPLE_DATA_2: list[tuple[int, str, date]] = [
    (4, "Hank", date(1993, 1, 1)),
    (5, "Suzan", date(1995, 5, 15)),
]
RELATION_SCHEMA = StructType(
    [
        StructField("id1", IntegerType(), nullable=False),
        StructField("id2", IntegerType(), nullable=True),
        StructField("rel_type", StringType(), nullable=True),
    ],
)
RELATION_DATA: list[tuple[int, int, str]] = [
    (1, 4, "friend"),
    (5, 2, "family"),
]
TRX_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("amount", DecimalType(10, 2), nullable=True),
        StructField("sign", StringType(), nullable=True),
    ],
)
TRX_DATA: list[tuple[int, Decimal, str]] = [
    (1, Decimal("4.10"), "DEBIT"),
    (2, Decimal("2.1"), "CREDIT"),
    (2, Decimal("22.1"), "CREDIT"),
    (2, Decimal("6.61"), "DEBIT"),
]


TABLE_PATH = Path(__file__).parent / "tables"
PLAN_PATH = Path(__file__).parent / "plans"


def create_tables_and_store(spark: SparkSession) -> None:
    clean_data_directory(TABLE_PATH)
    TABLE_PATH.mkdir(parents=True, exist_ok=True)

    store_dataframe(spark.createDataFrame(SAMPLE_DATA, SAMPLE_SCHEMA), TABLE_PATH / "sample_table")
    store_dataframe(spark.createDataFrame(SAMPLE_DATA_2, SAMPLE_SCHEMA_2), TABLE_PATH / "sample_table_2")
    store_dataframe(spark.createDataFrame(RELATION_DATA, RELATION_SCHEMA), TABLE_PATH / "relation_table")
    store_dataframe(spark.createDataFrame(TRX_DATA, TRX_SCHEMA), TABLE_PATH / "transaction_table")


def create_select_1_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    return get_query_plan(df)


def create_select_2_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df = df.select("id", "name")
    return get_query_plan(df)


def create_filter_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df = df.filter(f.col("id") > 1)
    return get_query_plan(df)


def create_filter_not_in_output_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df = df.filter(f.col("id") > 1).select("name")
    return get_query_plan(df)


def create_derive_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df = df.withColumn("age", f.col("age") + 1)
    df = df.withColumn("name_upper", f.upper(f.col("name")))
    df = df.withColumn("id_str", f.col("id").cast(StringType()))
    df = df.withColumn("id_and_name", f.concat_ws("~", f.col("id"), f.col("name")))
    df = df.withColumn("literal", f.lit("constant_value"))
    df = df.withColumn("none_value", f.lit(None).cast(StringType()))
    df = df.withColumn(
        "conditional_value",
        f.when(f.col("id") > 10, f.col("name")).when(f.col("id") < 0, f.upper("name")).otherwise(f.lit("Unknown")),  # noqa: PLR2004
    )
    df = df.withColumn("name_age", f.struct("name", "age"))
    df = df.withColumnRenamed("name", "original_name")
    return get_query_plan(df)


def create_union_plan(spark: SparkSession) -> str:
    df1 = spark.read.load(str(TABLE_PATH / "sample_table"))
    df2 = spark.read.load(str(TABLE_PATH / "sample_table_2"))
    df = df1.unionByName(df2, allowMissingColumns=True).select("name", "age", "birth_date")
    return get_query_plan(df)


def create_join_how_plan(spark: SparkSession, how: str) -> str:
    df1 = spark.read.load(str(TABLE_PATH / "sample_table"))
    df2 = spark.read.load(str(TABLE_PATH / "sample_table_2"))

    df_joined = (
        df1.alias("a")
        .join(df2.alias("b"), "id", how=how)
        .select("id", f.coalesce("a.name", "b.name").alias("name"), "age", "birth_date")
    )

    return get_query_plan(df_joined)


def create_join_2_plan(spark: SparkSession) -> str:
    df1 = spark.read.load(str(TABLE_PATH / "sample_table"))
    df2 = spark.read.load(str(TABLE_PATH / "sample_table_2"))
    df_rel = spark.read.load(str(TABLE_PATH / "relation_table"))

    df = df1.unionByName(df2, allowMissingColumns=True)

    df_joined = (
        df.alias("df_a")
        .join(df_rel.filter(f.col("rel_type") == "friend"), f.col("df_a.id") == df_rel.id1, how="inner")
        .join(df.alias("df_b"), f.col("df_b.id") == df_rel.id2, how="inner")
        .select(f.col("df_a.name").alias("name_a"), f.col("df_b.name").alias("name_b"))
    )

    return get_query_plan(df_joined)


def create_aggregation_1_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df_agg = df.groupBy().agg(f.avg("age").alias("avg_age"), f.max("age").alias("max_age"))
    return get_query_plan(df_agg)


def create_aggregation_2_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df_agg = df.groupBy("age").agg(f.count(f.lit(1)).alias("count"), f.max("name").alias("max_name"))
    return get_query_plan(df_agg)


def create_aggregation_3_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    trx = spark.read.load(str(TABLE_PATH / "transaction_table"))
    df_agg = (
        df.join(trx, on="id", how="left")
        .groupBy("id")
        .agg(
            f.count(f.lit(1)).alias("count"),
            f.sum(f.when(f.col("sign") == "DEBIT", f.col("amount")).otherwise(-f.col("amount"))).alias("total_amount"),
        )
    )
    return get_query_plan(df_agg)


# - aggregate
# - type conversion
# - from table / create df
# - all datatypes
def create_plans_and_store(spark: SparkSession) -> None:
    clean_data_directory(PLAN_PATH)
    PLAN_PATH.mkdir(parents=True, exist_ok=True)

    store_plan(create_select_1_plan(spark), PLAN_PATH / "select_1_plan.txt")
    store_plan(create_select_2_plan(spark), PLAN_PATH / "select_2_plan.txt")
    store_plan(create_filter_plan(spark), PLAN_PATH / "filter_plan.txt")
    store_plan(create_filter_not_in_output_plan(spark), PLAN_PATH / "filter_not_in_output_plan.txt")
    store_plan(create_derive_plan(spark), PLAN_PATH / "derive_plan.txt")
    store_plan(create_union_plan(spark), PLAN_PATH / "union_plan.txt")
    store_plan(create_join_how_plan(spark, "inner"), PLAN_PATH / "join_inner_plan.txt")
    store_plan(create_join_how_plan(spark, "left"), PLAN_PATH / "join_left_plan.txt")
    store_plan(create_join_how_plan(spark, "right"), PLAN_PATH / "join_rigth_plan.txt")
    store_plan(create_join_2_plan(spark), PLAN_PATH / "join_2_plan.txt")
    store_plan(create_aggregation_1_plan(spark), PLAN_PATH / "aggregation_1_plan.txt")
    store_plan(create_aggregation_2_plan(spark), PLAN_PATH / "aggregation_2_plan.txt")
    store_plan(create_aggregation_3_plan(spark), PLAN_PATH / "aggregation_3_plan.txt")
