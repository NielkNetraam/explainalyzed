from dataclasses import dataclass
from pathlib import Path


@dataclass
class SourceConfig:
    name: str
    path: Path
    format: str = "parquet"
