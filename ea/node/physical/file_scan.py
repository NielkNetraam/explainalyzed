import re

from ea.lineage import Lineage, SourceLineage
from ea.node.plan_node import PlanNode


class FileScanNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str, parameters: str) -> None:
        """Initialize a FileScan instance."""
        super().__init__(node_type, level, subset_id, parameters)
        pattern = r"(\w*)\s\[(.*)\]\s(.*)"
        matches = re.match(pattern, parameters)
        groups = matches.groups() if matches else ()

        self.source_type = groups[0]
        fields = groups[1].split(",") if groups[1] is not None and groups[1] != "" else []

        details = groups[2]
        detail_pattern = (
            r"Batched: (.*), DataFilters: \[(.*)\], Format: (\w*), "
            r"Location: .*\[(.*)\], PartitionFilters: \[(.*)\], "
            r"PushedFilters: \[(.*)\], ReadSchema: (.*)"
        )

        matches = re.match(detail_pattern, details)
        groups = matches.groups() if matches else ()
        self.batched: bool = groups[0] == "true"
        self.data_filters: list[str] = groups[1].split(",") if groups[1] is not None and groups[1] != "" else []
        self.format: str | None = groups[2] if groups[2] is not None and groups[2] != "" else None
        self.location: list[str] = groups[3].split(",") if groups[3] is not None and groups[3] != "" else []
        self.partition_filters: list[str] = groups[4].split(",") if groups[4] is not None and groups[4] != "" else []
        self.pushed_filters: list[str] = groups[5].split(",") if groups[5] is not None and groups[5] != "" else []
        self.read_schema: str | None = groups[6] if groups[6] is not None and groups[6] != "" else None

        self.fields: dict[str, Lineage] = {
            field.strip(): SourceLineage(field.strip(), location=self.location) for field in fields
        }

    def get_lineage(self) -> dict[str, Lineage]:
        return self.fields

    def __str__(self) -> str:
        """Return a string representation of the FileScan node."""
        return (
            f"{self.node_type} (Level: {self.level}) (Fields: {self.fields}) "
            f"(Source Type: {self.source_type}, Format: {self.format}, "
            f"Location: {self.location})"
        )
