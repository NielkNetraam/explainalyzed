from dataclasses import dataclass
from pathlib import Path


@dataclass
class TargetConfig:
    name: str
    path: Path
    format: str = "parquet"


target_config: dict[str, TargetConfig] = {
    "sample_table": TargetConfig(
        name="sample",
        path=Path("some_location") / "tables",
    ),
    "sample_table_2": TargetConfig(
        name="sample_2",
        path=Path("some_location").parent / "tables",
    ),
    "relation_table": TargetConfig(
        name="relation",
        path=Path("some_location").parent / "tables",
    ),
}
