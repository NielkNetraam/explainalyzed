import re

from ea.column_dependency import ColumnDependency, DerivedColumnDependency
from ea.node.plan_node import PlanNode
from ea.util import ID_PATTERN


class FilterNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        """Initialize a Filter instance."""
        super().__init__(node_type, level, subset_id, parameters)
        self.condition = parameters

        self.filter_fields = [f.lower() for f in set(re.findall(ID_PATTERN, parameters))]

    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        column_dependency: dict[str, ColumnDependency] = super().get_column_dependencies()

        filters: list[ColumnDependency] = [column_dependency[field_id] for field_id in self.filter_fields]

        return {k: DerivedColumnDependency(k, columns=[v], filters=filters) for k, v in column_dependency.items()}

    def __str__(self) -> str:
        """Return a string representation of the Filter node."""
        return f"{self.node_type} (Level: {self.level}) (Condition: {self.condition})"
