import re

from ea.lineage import DerivedLineage, Lineage
from ea.node.plan_node import PlanNode
from ea.util import strip_outer_parentheses


class ProjectNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str, parameters: str) -> None:
        super().__init__(node_type, level, subset_id, parameters)

        fields = strip_outer_parentheses(parameters[1:-1])

        self.fields: dict[str, list[str]] = {}
        for field in fields:
            if " AS " in field:
                name_part = field.rsplit(" AS ", 1)[1]
                function_part = field.rsplit(" AS ", 1)[0]

                pattern = r"([\w\d\_\-]*\#\d*)"
                src_fields = list(set(re.findall(pattern, function_part)))
                self.fields[name_part] = src_fields
            else:
                self.fields[field] = [field]

    def get_lineage(self) -> dict[str, Lineage]:
        lineage: dict[str, Lineage] = super().get_lineage()

        return {
            k: lineage[k] if k in lineage else DerivedLineage(k, fields=[lineage[fv] for fv in v])
            for k, v in self.fields.items()
        }
