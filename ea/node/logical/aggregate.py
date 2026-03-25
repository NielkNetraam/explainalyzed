import re

from ea.column_dependency import ColumnDependency, DerivedColumnDependency, SourceColumnDependency
from ea.node.plan_node import PlanNode
from ea.util import ID_PATTERN, split_field, strip_outer_parentheses


class AggregateNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        """Initialize a LogicalRDD instance."""
        super().__init__(node_type, level, subset_id, parameters)
        sections = re.findall(r"\[([^\[]*)\]", parameters)

        self.grouping_keys: list[str] = [] if len(sections) == 1 else list(set(re.findall(ID_PATTERN, sections[0])))

        fields: list[str] = strip_outer_parentheses(sections[len(sections) - 1])

        sf = split_field(fields)

        derived_fields: dict[str, str] = {
            name_part: function_part
            for function_part, name_part in (field.rsplit(" AS ", 1) for field in fields if " AS " in field)
        }
        self.derived_fields: dict[str, list[str]] = {
            key: sf[key] for key, value in derived_fields.items() if not ("__literal__" in value or "__none__" in value)
        }

        self.fields = sf

    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        column_dependency: dict[str, ColumnDependency] = super().get_column_dependencies()
        column_dependency["__literal__"] = SourceColumnDependency("literal", ["literal"])
        column_dependency["__none__"] = SourceColumnDependency("None", ["literal"])

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
