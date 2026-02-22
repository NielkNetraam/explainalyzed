from ea.column_dependency import ColumnDependency, DerivedColumnDependency
from ea.node.plan_node import PlanNode


class UnionNode(PlanNode):
    def mermaid(self, node_id_str: str) -> str:
        return f'{self.node_type}#{node_id_str}["{self.node_type}"]'

    def get_column_dependencies(self) -> dict[str, ColumnDependency]:
        combined_column_dependency = [child.get_column_dependencies() for child in self.children]

        names = combined_column_dependency[0].keys()
        zipped_column_dependency = zip(*[c.values() for c in combined_column_dependency], strict=False)

        column_dependency = {}
        for k, v in zip(names, zipped_column_dependency, strict=False):
            fields = list(set(v))
            column_dependency[k] = DerivedColumnDependency(k, columns=fields)

        return column_dependency
