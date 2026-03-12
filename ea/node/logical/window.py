import re

from ea.column_dependency import ColumnDependency, DerivedColumnDependency
from ea.node.plan_node import PlanNode
from ea.util import ID_PATTERN, strip_outer_parentheses


class WindowNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        """Initialize a WindowNode instance."""
        super().__init__(node_type, level, subset_id, parameters)
        sections = re.findall(r"\[([^\[]*)\]", parameters)

        fields: list[str] = strip_outer_parentheses(sections[0])

        derived_fields: dict[str, str] = {
            name_part: function_part
            for function_part, name_part in (field.rsplit(" AS ", 1) for field in fields if " AS " in field)
        }

        pattern = ID_PATTERN

        self.derived_fields: dict[str, list[str]] = {
            key: list(set(re.findall(pattern, d.rsplit("windowspecdefinition")[0]))) for key, d in derived_fields.items()
        }
        self.window_fields: dict[str, list[str]] = {
            key: list(set(re.findall(pattern, d.rsplit("windowspecdefinition")[1]))) for key, d in derived_fields.items()
        }

        self.fields: list[str] = [field if " AS " not in field else field.rsplit(" AS ", 1)[1] for field in fields]

    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        column_dependency: dict[str, ColumnDependency] = super().get_column_dependencies()

        window_fields: dict[str, list[ColumnDependency]] = {
            k: [column_dependency[f] for f in v] for k, v in self.window_fields.items()
        }
        derived_fields: dict[str, list[ColumnDependency]] = {
            k: [column_dependency[f] for f in v] for k, v in self.derived_fields.items()
        }

        derived_column_dependency = {
            k: DerivedColumnDependency(
                k,
                columns=derived_fields[k] if k in derived_fields else [column_dependency[k]],
                window_columns=window_fields.get(k),
            )
            for k in self.fields
        }

        return column_dependency | derived_column_dependency

    def __str__(self) -> str:
        """Return a string representation of the Window node."""
        return (
            f"{self.node_type} (Level: {self.level}) "
            f"(Window Fields: {self.window_fields}, Fields: {self.fields}, "
            f"Derived Fields: {self.derived_fields})"
        )
