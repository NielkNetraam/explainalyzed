from typing import TypeVar

from ea.lineage import Lineage


class PlanNode:
    def __init__(self, node_type: str, level: int, subset_id: str, parameters: str) -> None:
        """Initialize a Node instance."""
        self.node_type = node_type
        self.level = level
        self.subset_id = subset_id
        self.parameters = parameters
        self.children: list[PlanNode] = []

    def add_child(self, child: "PlanNode") -> None:
        """Add a child node."""
        self.children.append(child)

    def __str__(self) -> str:
        """Return a string representation of the node."""
        return f"{self.node_type} (Level: {self.level}): {self.parameters}"

    def print_node(self, indent: int = 0) -> None:
        print("   " * indent + self.__str__())  # noqa: T201
        for child in self.children:
            child.print_node(indent + 1)

    def get_lineage(self) -> dict[str, Lineage]:
        lineage: dict[str, Lineage] = {}
        for child in self.children:
            lineage = lineage | child.get_lineage()

        return lineage


PlanNodeType = TypeVar("PlanNodeType", bound=PlanNode)


class GenericNode(PlanNode):
    pass


GenericNodeType = TypeVar("GenericNodeType", bound=GenericNode)


class ColumnarToRowNode(PlanNode):
    pass


ColumnarToRowNodeType = TypeVar("ColumnarToRowNodeType", bound=ColumnarToRowNode)
