import re

from ea.lineage import Lineage, SourceLineage
from ea.node.plan_node import PlanNode


class RelationNode(PlanNode):
    def __init__(
        self,
        node_type: str,
        level: int,
        subset_id: str | None,
        parameters: str,
        mapping: dict[str, list[str]],
    ) -> None:
        """Initialize a FileScan instance."""
        super().__init__(node_type, level, subset_id, parameters)
        pattern = r"\[(.*)\]\s(\w*)"

        matches = re.match(pattern, parameters)
        groups = matches.groups() if matches else ()

        fields: list[str] = groups[0].split(",") if groups[0] is not None and groups[0] != "" else []
        self.source_type: str = groups[1]

        self.fields: dict[str, Lineage] = {
            field.strip(): SourceLineage(field.strip(), location=mapping[field.strip()])
            for field in fields
            if field.strip() in mapping
        }

    def get_lineage(self) -> dict[str, Lineage]:
        return self.fields

    def __str__(self) -> str:
        """Return a string representation of the FileScan node."""
        return f"{self.node_type} (Level: {self.level}) (Fields: {self.fields}) (Source Type: {self.source_type})"
