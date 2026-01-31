from ea.lineage import DerivedLineage, Lineage
from ea.node.plan_node import PlanNode
from ea.util import findall_column_ids


class JoinNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        """Initialize a Join instance."""
        super().__init__(node_type, level, subset_id, parameters)

        parameter_splitted = parameters.split(",", 1)
        self.join_type = parameter_splitted[0].strip()
        fields = parameter_splitted[1].strip()

        self.join_keys: list[str] = findall_column_ids(fields)

    def get_lineage(self) -> dict[str, Lineage]:
        lineage: dict[str, Lineage] = super().get_lineage()

        join_keys: list[Lineage] = [lineage[field_id] for field_id in self.join_keys]

        return {
            k: DerivedLineage(
                k,
                fields=[lineage[k]],
                join_keys=join_keys,
            )
            for k in lineage
        }

    def __str__(self) -> str:
        """Return a string representation of the Aggregate node."""
        return f"{self.node_type} (Level: {self.level}) (Join type: {self.join_type}), (Join keys: {self.join_keys})"
