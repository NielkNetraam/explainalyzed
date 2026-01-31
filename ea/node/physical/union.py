from ea.lineage import DerivedLineage, Lineage
from ea.node.plan_node import PlanNode


class UnionNode(PlanNode):
    def get_lineage(self) -> dict[str, Lineage]:
        combined_lineage = [child.get_lineage() for child in self.children]

        names = combined_lineage[0].keys()
        zipped_lineage = zip(*[c.values() for c in combined_lineage], strict=False)

        lineage = {}
        for k, v in zip(names, zipped_lineage, strict=False):
            fields = list(set(v))
            lineage[k] = DerivedLineage(k, fields=fields)

        return lineage
