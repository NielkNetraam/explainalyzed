import re

from ea.column_dependency import ColumnDependency, DerivedColumnDependency
from ea.node.plan_node import PlanNode
from ea.util import strip_outer_parentheses


class AggregateNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        """Initialize a LogicalRDD instance."""
        super().__init__(node_type, level, subset_id, parameters)
        sections = re.findall(r"\[([^\[]*)\]", parameters)

        self.grouping_keys: list[str] = [] if len(sections) == 1 else sections[0].split(", ")

        fields: list[str] = strip_outer_parentheses(sections[len(sections) - 1])

        derived_fields: dict[str, str] = {
            name_part: function_part
            for function_part, name_part in (field.rsplit(" AS ", 1) for field in fields if " AS " in field)
        }
        pattern = r"(\w[\w\d\_\-]*\#\d*)"
        self.derived_fields: dict[str, list[str]] = {key: list(set(re.findall(pattern, d))) for key, d in derived_fields.items()}

        self.fields: list[str] = [field if " AS " not in field else field.rsplit(" AS ", 1)[1] for field in fields]

    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        column_dependency: dict[str, ColumnDependency] = super().get_column_dependencies()

        grouping_keys: list[ColumnDependency] = [column_dependency[field_id] for field_id in self.grouping_keys]
        derived_fields: dict[str, list[ColumnDependency]] = {
            k: [column_dependency[f] for f in v] for k, v in self.derived_fields.items()
        }

        return {
            k: DerivedColumnDependency(
                k,
                columns=derived_fields[k] if k in derived_fields else [column_dependency[k]],
                grouping_keys=grouping_keys,
            )
            for k in self.fields
        }

    def __str__(self) -> str:
        """Return a string representation of the Aggregate node."""
        return (
            f"{self.node_type} (Level: {self.level}) "
            f"(Grouping key: {self.grouping_keys}, Fields: {self.fields}, "
            f"Derived Fields: {self.derived_fields})"
        )
