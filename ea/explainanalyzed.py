import re

from ea.node.logical.relation import RelationNode
from ea.node.physical.file_scan import FileScanNode
from ea.node.plan_node import GenericNode, PlanNode, PlanNodeType
from ea.node.registry import logical_class_registry, physical_class_registry


def _convert_line(
    line: str,
    class_registry: dict[str, type[PlanNodeType]],
    mapping: dict[str, list[str]] | None = None,
) -> PlanNode:
    pattern = r"(:?\s+)?([\+:]-)?\s*(\*\(\d*\))?\s*(\w*)\s*(.*)"
    matches = re.match(pattern, line)

    if matches is None:
        msg = "Invalid line format"
        raise ValueError(msg)

    groups = matches.groups()
    level = (int(len(groups[0]) / 3) if groups[0] is not None else 0) + (1 if groups[1] in ["+-", ":-"] else 0)
    subset_id = groups[2]
    node_type: str = groups[3]
    parameters: str = groups[4]

    if node_type in class_registry:
        if node_type == "Relation":
            return RelationNode(node_type, level, subset_id, parameters, mapping=mapping if mapping else {})
        return class_registry[node_type](node_type, level, subset_id, parameters)
    return GenericNode(node_type, level, subset_id, parameters)


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

    def __init__(self, plan_data: list[str]) -> None:
        self.plan_data = [line.rstrip() for line in plan_data]

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
        plan_lines = self._get_section(self.PARSED_LOGICAL_PLAN, self.ANALYZED_LOGICAL_PLAN)
        return _convert_lines(plan_lines, class_registry=logical_class_registry, mapping=mapping)

    def get_analyzed_logical_plan(self, mapping: dict[str, list[str]]) -> PlanNode:
        plan_lines = self._get_section(self.ANALYZED_LOGICAL_PLAN, self.OPTIMIZED_LOGICAL_PLAN, shift=1)
        return _convert_lines(plan_lines, class_registry=logical_class_registry, mapping=mapping)

    def get_optimized_logical_plan(self, mapping: dict[str, list[str]]) -> PlanNode:
        plan_lines = self._get_section(self.OPTIMIZED_LOGICAL_PLAN, self.PHYSICAL_PLAN)
        return _convert_lines(plan_lines, class_registry=logical_class_registry, mapping=mapping)

    def get_physical_plan(self) -> PlanNode:
        plan_lines = self._get_section(self.PHYSICAL_PLAN, None)
        return _convert_lines(plan_lines, class_registry=physical_class_registry)

    def get_physical_field_table_mapping(self) -> dict[str, list[str]]:
        plan_lines = self._get_section(self.PHYSICAL_PLAN, None)
        return _convert_lines_to_mapping(plan_lines)
