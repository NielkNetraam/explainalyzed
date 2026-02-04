from typing import TypeVar

from ea.column_dependency import ColumnDependency
from ea.lineage import Column, Lineage, Table, TableLineage


class PlanNode:
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
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

    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        column_dependency: dict[str, ColumnDependency] = {}
        for child in self.children:
            column_dependency = column_dependency | child.get_column_dependencies()

        return column_dependency

    def get_lineage(self, dataset_name: str) -> Lineage:
        column_lineage = {cl for v in self.get_column_dependencies().values() for cl in v.get_column_lineage(dataset_name)}

        tables: set[Table] = {
            *{cl.source_column.table for cl in column_lineage},
            *{cl.target_column.table for cl in column_lineage},
        }
        source_columns: set[Column] = {cl.source_column for cl in column_lineage}
        target_columns: set[Column] = {cl.target_column for cl in column_lineage}
        table_lineage = {
            TableLineage(source_table=cl.source_column.table, target_table=cl.target_column.table) for cl in column_lineage
        }
        return Lineage(
            tables=tables,
            source_columns=source_columns,
            target_columns=target_columns,
            table_lineage=table_lineage,
            column_lineage=column_lineage,
        )


PlanNodeType = TypeVar("PlanNodeType", bound=PlanNode)


class GenericNode(PlanNode):
    pass


GenericNodeType = TypeVar("GenericNodeType", bound=GenericNode)


class ColumnarToRowNode(PlanNode):
    pass


ColumnarToRowNodeType = TypeVar("ColumnarToRowNodeType", bound=ColumnarToRowNode)
