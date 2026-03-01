from pathlib import Path
from typing import TYPE_CHECKING

from sample_project.config import Config, SourceConfig
from sample_project.transformation import business_logic, prep_relation, prep_sample, prep_sample_2
from sample_project.util import read_source, set_spark_conf_for_lineage, write_and_read

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

source_config: dict[str, SourceConfig] = {
    "sample_table": SourceConfig(
        name="sample_table",
        path=Path(__file__).parent.parent / "data" / "tables",
    ),
    "sample_table_2": SourceConfig(
        name="sample_table_2",
        path=Path(__file__).parent.parent / "data" / "tables",
    ),
    "relation_table": SourceConfig(
        name="relation_table",
        path=Path(__file__).parent.parent / "data" / "tables",
    ),
}


def builder(config: Config, source_config: dict[str, SourceConfig]) -> None:
    if config.lineage:
        set_spark_conf_for_lineage()

    # read sources
    sources: dict[str, DataFrame] = {name: read_source(source_config) for name, source_config in source_config.items()}

    # prep sources and store intermediate results
    sample_df = prep_sample(sources["sample_table"])
    sample_2_df = prep_sample_2(sources["sample_table_2"])
    relation_df = prep_relation(sources["relation_table"])

    sample_df = write_and_read(sample_df, "sample_table_prepped", config, temporary=True)
    sample_2_df = write_and_read(sample_2_df, "sample_table_2_prepped", config, temporary=True)
    relation_df = write_and_read(relation_df, "relation_table_prepped", config, temporary=True)

    # perform business logic
    business_logic_df = business_logic(sample_df, sample_2_df, relation_df)

    # store results

    _ = write_and_read(business_logic_df, "business_logic_result", config)
