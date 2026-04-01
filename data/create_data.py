from datetime import date
from decimal import Decimal
from pathlib import Path

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType

from data.utils import clean_data_directory, get_query_plan, store_dataframe, store_json, store_plan

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
JSON_PATH = Path(__file__).parent / "json"
PLAN_PATH = Path(__file__).parent / "plans"


def create_tables_and_store(spark: SparkSession) -> None:
    clean_data_directory(TABLE_PATH, create_if_not_exists=True)

    store_dataframe(spark.createDataFrame(SAMPLE_DATA, SAMPLE_SCHEMA), TABLE_PATH / "sample_table")
    store_dataframe(spark.createDataFrame(SAMPLE_DATA_2, SAMPLE_SCHEMA_2), TABLE_PATH / "sample_table_2")
    store_dataframe(spark.createDataFrame(RELATION_DATA, RELATION_SCHEMA), TABLE_PATH / "relation_table")
    store_dataframe(spark.createDataFrame(TRX_DATA, TRX_SCHEMA), TABLE_PATH / "transaction_table")


def create_json_and_store(spark: SparkSession) -> None:
    clean_data_directory(JSON_PATH, create_if_not_exists=True)

    store_json(spark.createDataFrame(SAMPLE_DATA, SAMPLE_SCHEMA), JSON_PATH / "sample_table")
    store_json(spark.createDataFrame(SAMPLE_DATA_2, SAMPLE_SCHEMA_2), JSON_PATH / "sample_table_2")
    store_json(spark.createDataFrame(RELATION_DATA, RELATION_SCHEMA), JSON_PATH / "relation_table")
    store_json(spark.createDataFrame(TRX_DATA, TRX_SCHEMA), JSON_PATH / "transaction_table")


def create_select_1_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    return get_query_plan(df)


def select_1_from_json_plan(spark: SparkSession) -> DataFrame:
    return spark.read.json(str(JSON_PATH / "sample_table"))


def create_select_2_plan(spark: SparkSession) -> str:
    lit1 = "literal_string"
    lit2 = {"range": {"start": 1, " end": 2}, "length": 2}

    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df = df.select("id", "name", f.lit(lit1).alias("lit1"), f.lit(lit2.__repr__()).alias("lit2"), f.upper("Name"))
    return get_query_plan(df)


def select_re_query(spark: SparkSession) -> DataFrame:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df = df.select("id", f.col("name").alias("name_re1"), f.col("name").alias("name_re2"))

    replace_strs = ["test", "sample", "example"]
    replace_re = "|".join(f"\\b{replace_str}\\b" for replace_str in replace_strs)
    df = df.withColumn("name_re1", f.regexp_replace("name_re1", replace_re, ""))

    for replace_str in replace_strs:
        df = df.withColumn("name_re2", f.regexp_replace("name_re2", f"\\b{replace_str}\\b", ""))

    return df


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


def join_cross_query(spark: SparkSession, *, broadcast: bool) -> DataFrame:
    df1 = spark.read.load(str(TABLE_PATH / "sample_table"))
    df2 = spark.read.load(str(TABLE_PATH / "transaction_table"))

    if broadcast:
        df2 = f.broadcast(df2)

    return df1.alias("a").join(df2.alias("b"), how="cross").select("name", "b.*")


def join_2_query(spark: SparkSession, *, broadcast: bool = False) -> DataFrame:
    df1 = spark.read.load(str(TABLE_PATH / "sample_table"))
    df2 = spark.read.load(str(TABLE_PATH / "sample_table_2"))
    df_rel = spark.read.load(str(TABLE_PATH / "relation_table"))

    if broadcast:
        df_rel = f.broadcast(df_rel)

    df = df1.unionByName(df2, allowMissingColumns=True)

    return (
        df.alias("df_a")
        .join(df_rel.filter(f.col("rel_type") == "friend"), f.col("df_a.id") == df_rel.id1, how="inner")
        .join(df.alias("df_b"), f.col("df_b.id") == df_rel.id2, how="inner")
        .select(f.col("df_a.name").alias("name_a"), f.col("df_b.name").alias("name_b"))
    )


def transform_query(spark: SparkSession) -> DataFrame:
    def alternate(x: Column, i: Column) -> Column:
        return f.when(i % 2 == 0, x).otherwise(f.upper(x))

    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df = df.groupBy("age").agg(f.collect_set(f.col("name")).alias("names"))
    return df.withColumn("names_upper", f.transform("names", lambda name: f.upper(name))).withColumn(
        "names_upper2",
        f.transform("names", alternate),
    )


def explode_query(spark: SparkSession) -> DataFrame:
    df = transform_query(spark)
    df = df.withColumn("new_name", f.explode("names_upper2"))
    df = df.withColumn("m", f.create_map("new_name", "age"))
    df = df.select("*", f.explode("m"))
    return df


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


def aggregation_4_query(spark: SparkSession) -> DataFrame:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    df = df.withColumn("year", f.lit(2025))
    return df.groupBy("year").agg(f.avg("age").alias("avg_age"), f.max("age").alias("max_age"))


def aggregation_5_query(spark: SparkSession) -> DataFrame:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))

    return df.groupBy("name").pivot("id").agg(f.avg("age"))


def create_rdd_plan(spark: SparkSession) -> str:
    df = spark.read.load(str(TABLE_PATH / "sample_table"))
    rdd = df.rdd
    df2 = spark.createDataFrame(rdd, schema=df.schema)
    return get_query_plan(df2)


def split_and_union(df: DataFrame, split: str, join_df: DataFrame, colum_to_add: str) -> DataFrame:
    df1 = df.filter(f.col("name_a").rlike(split))
    df2 = (
        df.filter(~f.col("name_a").rlike(split))
        .join(join_df, on=(f.col("name_a") == f.col("name")), how="left")
        .withColumnRenamed("id", colum_to_add)
        .drop("name", "age")
    )
    return df1.unionByName(df2, allowMissingColumns=True)


def union_forest_query(spark: SparkSession, *, cache: bool = False) -> DataFrame:
    df1 = join_2_query(spark)
    if cache:
        df1 = df1.cache()

    df2 = spark.read.load(str(TABLE_PATH / "sample_table"))

    df1 = split_and_union(df1, "simple", df2, "simple_id")
    return split_and_union(df1, "complex", df2, "complex_id")


def window_column_query(spark: SparkSession) -> DataFrame:
    df = spark.read.load(str(TABLE_PATH / "transaction_table"))

    return df.withColumn(
        "max_amount_por",
        f.max("amount").over(
            Window.partitionBy("id").orderBy("sign").rowsBetween(Window.currentRow + 1, Window.unboundedFollowing),
        ),
    )


def window_column_2_query(spark: SparkSession) -> DataFrame:
    df = spark.read.load(str(TABLE_PATH / "transaction_table"))

    return df.withColumn(
        "max_amount_por",
        f.max("amount").over(
            Window.partitionBy("id").orderBy("sign").rowsBetween(Window.currentRow + 1, Window.unboundedFollowing),
        ),
    ).withColumn(
        "max_amount_po",
        f.max("amount").over(
            Window.partitionBy("id").orderBy("sign"),
        ),
    )


def window_column_3_query(spark: SparkSession) -> DataFrame:
    df = spark.read.load(str(TABLE_PATH / "transaction_table"))

    return (
        df.withColumn(
            "max_amount_por",
            f.max("amount").over(
                Window.partitionBy("id").orderBy("sign").rowsBetween(Window.currentRow + 1, Window.unboundedFollowing),
            ),
        )
        .withColumn(
            "max_amount_or",
            f.max("amount").over(
                Window.orderBy("sign").rowsBetween(Window.currentRow + 1, Window.unboundedFollowing),
            ),
        )
        .withColumn(
            "max_amount_po",
            f.max("amount").over(
                Window.partitionBy("id").orderBy("sign"),
            ),
        )
        .withColumn(
            "count_amount_por",
            f.count("amount").over(
                Window.partitionBy("id").rowsBetween(Window.currentRow + 1, Window.unboundedFollowing),
            ),
        )
        .withColumn(
            "count_amount_or",
            f.count("amount").over(
                Window.rowsBetween(Window.currentRow + 1, Window.unboundedFollowing),
            ),
        )
        .withColumn(
            "count_amount_po",
            f.count("amount").over(
                Window.partitionBy("id"),
            ),
        )
        .withColumn(
            "count_por",
            f.count(f.lit(1)).over(
                Window.partitionBy("id").rowsBetween(Window.currentRow + 1, Window.unboundedFollowing),
            ),
        )
        .withColumn(
            "count_or",
            f.count(f.lit(1)).over(
                Window.rowsBetween(Window.currentRow + 1, Window.unboundedFollowing),
            ),
        )
        .withColumn(
            "count_po",
            f.count(f.lit(1)).over(
                Window.partitionBy("id"),
            ),
        )
    )


def window_column_4_query(spark: SparkSession) -> DataFrame:
    df = spark.read.load(str(TABLE_PATH / "transaction_table"))

    return df.withColumn(
        "row_number",
        f.row_number().over(Window.partitionBy("id").orderBy("sign")),
    ).filter(f.col("row_number") == f.lit(1))


# - aggregate
# - type conversion
# - from table / create df
# - all datatypes
def create_plans_and_store(spark: SparkSession) -> None:
    clean_data_directory(PLAN_PATH)
    PLAN_PATH.mkdir(parents=True, exist_ok=True)

    store_plan(create_select_1_plan(spark), PLAN_PATH / "select_1_plan.txt")
    store_plan(create_select_2_plan(spark), PLAN_PATH / "select_2_plan.txt")
    store_plan(get_query_plan(select_re_query(spark)), PLAN_PATH / "select_re_plan.txt")
    store_plan(create_filter_plan(spark), PLAN_PATH / "filter_plan.txt")
    store_plan(create_filter_not_in_output_plan(spark), PLAN_PATH / "filter_not_in_output_plan.txt")
    store_plan(create_derive_plan(spark), PLAN_PATH / "derive_plan.txt")
    store_plan(create_union_plan(spark), PLAN_PATH / "union_plan.txt")
    store_plan(create_join_how_plan(spark, "inner"), PLAN_PATH / "join_inner_plan.txt")
    store_plan(create_join_how_plan(spark, "left"), PLAN_PATH / "join_left_plan.txt")
    store_plan(create_join_how_plan(spark, "right"), PLAN_PATH / "join_right_plan.txt")
    store_plan(get_query_plan(join_cross_query(spark, broadcast=False)), PLAN_PATH / "join_cross_plan.txt")
    store_plan(get_query_plan(join_cross_query(spark, broadcast=True)), PLAN_PATH / "join_cross_broadcast_plan.txt")
    store_plan(get_query_plan(join_2_query(spark)), PLAN_PATH / "join_2_plan.txt")
    store_plan(get_query_plan(join_2_query(spark, broadcast=True)), PLAN_PATH / "join_2_broadcast_plan.txt")
    store_plan(create_aggregation_1_plan(spark), PLAN_PATH / "aggregation_1_plan.txt")
    store_plan(create_aggregation_2_plan(spark), PLAN_PATH / "aggregation_2_plan.txt")
    store_plan(create_aggregation_3_plan(spark), PLAN_PATH / "aggregation_3_plan.txt")
    store_plan(get_query_plan(aggregation_4_query(spark)), PLAN_PATH / "aggregation_4_plan.txt")
    store_plan(get_query_plan(aggregation_5_query(spark)), PLAN_PATH / "aggregation_5_plan.txt")
    store_plan(create_rdd_plan(spark), PLAN_PATH / "rdd_plan.txt")
    store_plan(get_query_plan(union_forest_query(spark)), PLAN_PATH / "union_forest_plan.txt")
    store_plan(get_query_plan(union_forest_query(spark, cache=True)), PLAN_PATH / "union_forest_cache_plan.txt")
    store_plan(get_query_plan(window_column_query(spark)), PLAN_PATH / "window_column_plan.txt")
    store_plan(get_query_plan(window_column_2_query(spark)), PLAN_PATH / "window_column_2_plan.txt")
    store_plan(get_query_plan(window_column_3_query(spark)), PLAN_PATH / "window_column_3_plan.txt")
    store_plan(get_query_plan(window_column_4_query(spark)), PLAN_PATH / "window_column_4_plan.txt")
    store_plan(get_query_plan(transform_query(spark)), PLAN_PATH / "transform_plan.txt")
    store_plan(get_query_plan(explode_query(spark)), PLAN_PATH / "explode_plan.txt")

    store_plan(get_query_plan(select_1_from_json_plan(spark)), PLAN_PATH / "select_1_from_json_plan.txt")
