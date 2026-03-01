from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def prep_sample(df: DataFrame) -> DataFrame:
    return df.filter(f.col("id") > 1).withColumn("name_upper", f.upper(f.col("name"))).select("id", "name_upper")


def prep_sample_2(df: DataFrame) -> DataFrame:
    return (
        df.filter(f.col("id") < 10)  # noqa: PLR2004
        .withColumn("name_upper", f.upper(f.col("name")))
        .select("id", "name_upper")
    )


def prep_relation(df: DataFrame) -> DataFrame:
    return (
        df.filter(f.col("id1") > 1)
        .filter(f.col("id2") < 10)  # noqa: PLR2004
        .filter(f.col("rel_type") == "friend")
    )


def business_logic(sample_df: DataFrame, sample_2_df: DataFrame, relation_df: DataFrame) -> DataFrame:
    df = sample_df.unionByName(sample_2_df, allowMissingColumns=True)

    return (
        df.alias("df_a")
        .join(relation_df.filter(f.col("rel_type") == "friend"), f.col("df_a.id") == relation_df.id1, how="inner")
        .join(df.alias("df_b"), f.col("df_b.id") == relation_df.id2, how="inner")
        .select(f.col("df_a.name_upper").alias("name_a"), f.col("df_b.name_upper").alias("name_b"))
    )
