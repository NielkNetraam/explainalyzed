from ea.node.plan_node import PlanNode


class WindowGroupLimitNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        """Initialize a WindowGroupLimit instance."""
        super().__init__(node_type, level, subset_id, parameters)

    def __str__(self) -> str:
        """Return a string representation of the Filter node."""
        return f"{self.node_type} (Level: {self.level})"
