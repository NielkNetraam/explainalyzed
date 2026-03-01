from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    build_location: Path
    lineage: bool = False
    plan_location: Path | None = None


@dataclass
class SourceConfig:
    name: str
    path: Path
    format: str = "parquet"
