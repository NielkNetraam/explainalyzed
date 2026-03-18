import re

from ea.column_dependency import ColumnDependency, DerivedColumnDependency
from ea.node.plan_node import PlanNode
from ea.util import ID_PATTERN, strip_outer_parentheses


class GenerateNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        """Initialize a Logical Generate Node instance."""
        super().__init__(node_type, level, subset_id, parameters)

        sections = re.split(r', ?(?![^\[]*\])', parameters)
        self.fields = list(set(re.findall(ID_PATTERN, sections[2])))
        base_fields = list(set(re.findall(ID_PATTERN, sections[0])))
        self.derived_fields = {field: base_fields for field in self.fields}


    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        column_dependency: dict[str, ColumnDependency] = super().get_column_dependencies()

        derived_fields: dict[str, list[ColumnDependency]] = {
            k: [column_dependency[f] for f in v] for k, v in self.derived_fields.items()
        }


        generate_dependency = {
            k: DerivedColumnDependency(
                k,
                columns=derived_fields[k] if k in derived_fields else [column_dependency[k]],
            )
            for k in self.fields
        }

        return column_dependency | generate_dependency

    def __str__(self) -> str:
        """Return a string representation of the Aggregate node."""
        return (
            f"{self.node_type} (Level: {self.level}) "
            f"(Fields: {self.fields}, Derived Fields: {self.derived_fields})"
        )
