from typing import Literal

from ea.column_dependency import ColumnDependency, DerivedColumnDependency
from ea.node.plan_node import PlanNode
from ea.util import findall_column_ids


class JoinNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        """Initialize a Join instance."""
        super().__init__(node_type, level, subset_id, parameters)
        self.broadcast: Literal["left", "right"] | None = None

        parameter_splitted = parameters.split(",", 1)
        self.join_type = parameter_splitted[0].strip()
        fields = parameter_splitted[1].strip()

        if "broadcast" in fields:
            self.broadcast = "right" if "rightHint" in fields else "left"

        self.join_keys: list[str] = findall_column_ids(fields)

    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        column_dependency: dict[str, ColumnDependency] = super().get_column_dependencies()

        join_keys: list[ColumnDependency] = [column_dependency[field_id] for field_id in self.join_keys]

        return {
            k: DerivedColumnDependency(
                k,
                columns=[column_dependency[k]],
                join_keys=join_keys,
            )
            for k in column_dependency
        }

    def __str__(self) -> str:
        """Return a string representation of the Aggregate node."""
        return f"{self.node_type} (Level: {self.level}) (Join type: {self.join_type}), (Join keys: {self.join_keys})"
