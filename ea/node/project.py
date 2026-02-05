import re

from ea.column_dependency import ColumnDependency, DerivedColumnDependency, SourceColumnDependency
from ea.node.plan_node import PlanNode
from ea.util import strip_outer_parentheses


class ProjectNode(PlanNode):
    def __init__(self, node_type: str, level: int, subset_id: str | None, parameters: str) -> None:
        super().__init__(node_type, level, subset_id, parameters)

        fields = strip_outer_parentheses(parameters[1:-1])

        self.fields: dict[str, list[str]] = {}
        for field in fields:
            if " AS " in field:
                name_part = field.rsplit(" AS ", 1)[1]
                function_part = field.rsplit(" AS ", 1)[0]

                pattern = r"(\w[\w\d\_\-]*\#\d*)"
                src_fields = list(set(re.findall(pattern, function_part)))
                self.fields[name_part] = (
                    src_fields if len(src_fields) > 0 else ["__none__" if function_part == "null" else "__literal__"]
                )
            else:
                self.fields[field] = [field]

    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        column_dependency: dict[str, ColumnDependency] = super().get_column_dependencies()
        column_dependency["__literal__"] = SourceColumnDependency("literal", ["literal"])
        column_dependency["__none__"] = SourceColumnDependency("None", ["literal"])

        return {
            k: column_dependency[k]
            if k in column_dependency
            else DerivedColumnDependency(k, columns=[column_dependency[fv] for fv in v])
            for k, v in self.fields.items()
        }
