import re

from ea.lineage import Lineage
from ea.node.logical.relation import RelationNode
from ea.node.physical.file_scan import FileScanNode
from ea.node.plan_node import GenericNode, PlanNode, PlanNodeType
from ea.node.registry import logical_class_registry, physical_class_registry


def _split_line(line: str) -> tuple[int, str | None, str, str]:
    pattern = r"([:\s\+-]*)?(\*\(\d*\))?\s*(\w*)\s*(.*)"
    matches = re.match(pattern, line)

    if matches is None:
        msg = "Invalid line format"
        raise ValueError(msg)

    groups = matches.groups()
    level = int(len(groups[0]) / 3) if groups[0] is not None else 0
    subset_id: str | None = groups[1]
    node_type: str = groups[2]
    remainder: str = groups[3]

    return level, subset_id, node_type, remainder


def _convert_line(
    line: str,
    class_registry: dict[str, type[PlanNodeType]],
    mapping: dict[str, list[str]] | None = None,
) -> PlanNode:
    level, subset_id, node_type, remainder = _split_line(line)

    if node_type in class_registry:
        if node_type == "Relation":
            return RelationNode(node_type, level, subset_id, remainder, mapping=mapping if mapping else {})
        return class_registry[node_type](node_type, level, subset_id, remainder)
    return GenericNode(node_type, level, subset_id, remainder)


def _convert_lines(
    lines: list[str],
    class_registry: dict[str, type[PlanNodeType]],
    mapping: dict[str, list[str]] | None = None,
) -> PlanNode:
    level_node: dict[int, PlanNode] = {}

    for line in lines:
        converted_node = _convert_line(line, class_registry=class_registry, mapping=mapping)
        level_node[converted_node.level] = converted_node

        if converted_node.level > 0:
            level_node[converted_node.level - 1].add_child(converted_node)

    return level_node[0]


def _convert_lines_to_mapping(lines: list[str]) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}

    for line in lines:
        if "FileScan" in line:
            parameters = line.split("FileScan")[1].strip()
            file_scan = FileScanNode("FileScan", 0, "", parameters)

            _mapping = {field.column_id: field.location for field in file_scan.fields.values()}  # ty:ignore[unresolved-attribute]
            mapping.update(_mapping)
    return mapping


class ExplainAnalyzed:
    PARSED_LOGICAL_PLAN = "== Parsed Logical Plan =="
    ANALYZED_LOGICAL_PLAN = "== Analyzed Logical Plan =="
    OPTIMIZED_LOGICAL_PLAN = "== Optimized Logical Plan =="
    PHYSICAL_PLAN = "== Physical Plan =="

    def __init__(self, dataset_name: str, plan_data: list[str]) -> None:
        self.dataset_name = dataset_name
        self.plan_data = [line.rstrip() for line in plan_data]
        self._parsed_logical_plan: PlanNode | None = None
        self._analyzed_logical_plan: PlanNode | None = None
        self._optimized_logical_plan: PlanNode | None = None
        self._physical_plan: PlanNode | None = None
        self._lineage: Lineage | None = None

    def _get_index(self, marker: str) -> int:
        try:
            return self.plan_data.index(marker)
        except ValueError:
            return -1

    def _get_section(self, start_marker: str, end_marker: str | None, shift: int = 0) -> list[str]:
        start_index = self._get_index(start_marker)

        if start_index == -1:
            return []

        start_index += 1 + shift  # Move to the line after the marker

        end_index = self.plan_data.index(end_marker) - 1 if end_marker else len(self.plan_data)

        return self.plan_data[start_index:end_index]

    def get_parsed_logical_plan(self, mapping: dict[str, list[str]]) -> PlanNode:
        if self._parsed_logical_plan is None:
            plan_lines = self._get_section(self.PARSED_LOGICAL_PLAN, self.ANALYZED_LOGICAL_PLAN)
            self._parsed_logical_plan = _convert_lines(plan_lines, class_registry=logical_class_registry, mapping=mapping)

        return self._parsed_logical_plan

    def get_analyzed_logical_plan(self, mapping: dict[str, list[str]]) -> PlanNode:
        if self._analyzed_logical_plan is None:
            plan_lines = self._get_section(self.ANALYZED_LOGICAL_PLAN, self.OPTIMIZED_LOGICAL_PLAN, shift=1)
            self._analyzed_logical_plan = _convert_lines(plan_lines, class_registry=logical_class_registry, mapping=mapping)

        return self._analyzed_logical_plan

    def get_optimized_logical_plan(self) -> PlanNode:
        if self._optimized_logical_plan is None:
            mapping = self._get_physical_field_table_mapping()
            plan_lines = self._get_section(self.OPTIMIZED_LOGICAL_PLAN, self.PHYSICAL_PLAN)
            self._optimized_logical_plan = _convert_lines(plan_lines, class_registry=logical_class_registry, mapping=mapping)

        return self._optimized_logical_plan

    def get_physical_plan(self) -> PlanNode:
        if self._physical_plan is None:
            plan_lines = self._get_section(self.PHYSICAL_PLAN, None)
            self._physical_plan = _convert_lines(plan_lines, class_registry=physical_class_registry)

        return self._physical_plan

    def _get_physical_field_table_mapping(self) -> dict[str, list[str]]:
        plan_lines = self._get_section(self.PHYSICAL_PLAN, None)
        return _convert_lines_to_mapping(plan_lines)

    def get_lineage(self) -> Lineage:
        if self._lineage is None:
            tree = self.get_optimized_logical_plan()

            self._lineage = tree.get_lineage(self.dataset_name)

        return self._lineage

    def mermaid(self) -> str:
        def nodes(node: PlanNode, node_id: str = "", indent: int = 0) -> str:
            node_str = ""
            node_id_str = "1" if node_id == "" else node_id
            node_str = "    " * indent + node.mermaid(node_id_str) + f":::{node.node_type.upper()}\n"

            for idx, child in enumerate(node.children):
                node_str += nodes(child, node_id=f"{node_id_str}{idx + 1}", indent=indent)

            return node_str

        def edges(node: PlanNode, node_id: str = "", indent: int = 0) -> str:
            edge_str = ""
            node_id_str = "1" if node_id == "" else node_id

            for idx, child in enumerate(node.children):
                edge_str += "    " * indent + f"{node.node_type}#{node_id_str} --> {child.node_type}#{node_id_str}{idx + 1}\n"
                edge_str += edges(child, node_id=f"{node_id_str}{idx + 1}", indent=indent + 1)

            return edge_str

        olp = self.get_optimized_logical_plan()

        mermaid_str = (
            "flowchart TD\n"
            "    classDef PROJECT stroke:Blue,fill:LightBlue,color:black\n"
            "    classDef FILTER stroke:Green,fill:LightGreen,color:black\n"
            "    classDef UNION stroke:Purple,fill:Violet,color:black\n"
            "    classDef JOIN stroke:DarkOrange,fill:Orange,color:black\n"
            "    classDef RELATION stroke:Yellow,fill:Gold,color:black\n"
            "    classDef AGGREGATE stroke:Brown,fill:Chocolate,color:black\n"
            "\n"
        )

        mermaid_str += nodes(olp, indent=1) + "\n"
        mermaid_str += edges(olp, indent=1)
        return mermaid_str
