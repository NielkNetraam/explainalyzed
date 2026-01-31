import re

from ea.node.plan_node import PlanNode


class LogicalRDDNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str, parameters: str) -> None:
        """Initialize a LogicalRDD instance."""
        super().__init__(node_type, level, subset_id, parameters)
        pattern = r"\[(.*)\],\s.*"
        matches = re.match(pattern, parameters)
        self.fields = matches.groups()[0].split(", ") if matches else []

    def __str__(self) -> str:
        """Return a string representation of the LogicalRDD node."""
        return f"{self.node_type} (Level: {self.level}) (Fields: {self.fields})"
