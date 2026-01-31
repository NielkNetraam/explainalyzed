import re

from ea.lineage import DerivedLineage, Lineage
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
        pattern = r"([\w\d\_\-]*\#\d*)"
        self.derived_fields: dict[str, list[str]] = {key: list(set(re.findall(pattern, d))) for key, d in derived_fields.items()}

        self.fields: list[str] = [field if " AS " not in field else field.rsplit(" AS ", 1)[1] for field in fields]

    def get_lineage(self) -> dict[str, Lineage]:
        lineage: dict[str, Lineage] = super().get_lineage()

        grouping_keys: list[Lineage] = [lineage[field_id] for field_id in self.grouping_keys]
        derived_fields: dict[str, list[Lineage]] = {k: [lineage[f] for f in v] for k, v in self.derived_fields.items()}

        return {
            k: DerivedLineage(
                k,
                fields=derived_fields[k] if k in derived_fields else [lineage[k]],
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
