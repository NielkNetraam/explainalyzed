import re

from ea.lineage import DerivedLineage, Lineage
from ea.node.plan_node import PlanNode


class FilterNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str, parameters: str) -> None:
        """Initialize a Filter instance."""
        super().__init__(node_type, level, subset_id, parameters)
        self.condition = parameters

        pattern = r"([\w\d\_\-]*\#\d*)"
        self.filter_fields = list(set(re.findall(pattern, parameters)))

    def get_lineage(self) -> dict[str, Lineage]:
        lineage: dict[str, Lineage] = super().get_lineage()

        filters: list[Lineage] = [lineage[field_id] for field_id in self.filter_fields]

        return {k: DerivedLineage(k, fields=[v], filters=filters) for k, v in lineage.items()}

    def __str__(self) -> str:
        """Return a string representation of the Filter node."""
        return f"{self.node_type} (Level: {self.level}) (Condition: {self.condition})"
