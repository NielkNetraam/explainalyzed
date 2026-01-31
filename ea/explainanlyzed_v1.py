import re

from ea.util import strip_outer_parentheses


class PlanNode:
    def __init__(self, node_type: str, level: int, parameters: str) -> None:
        """Initialize a Node instance."""
        self.node_type = node_type
        self.level = level
        self.parameters = parameters
        self.children: list[PlanNode] = []

    def add_child(self, child: "PlanNode") -> None:
        """Add a child node."""
        self.children.append(child)

    def __str__(self) -> str:
        """Return a string representation of the node."""
        return f"{self.node_type} (Level: {self.level}): {self.parameters}"

    def print_node(self, indent: int = 0) -> None:
        print("   " * indent + self.__str__())
        for child in self.children:
            child.print_node(indent + 1)


class GenericNode(PlanNode):
    pass


class Aggregate(PlanNode):
    def __init__(self, node_type: str, level: int, parameters: str) -> None:
        """Initialize a LogicalRDD instance."""
        super().__init__(node_type, level, parameters)
        pattern = r"\[(.*)\],\s\[(.*)\]"
        matches = re.match(pattern, parameters)
        self.grouping = matches.groups()[0].split(", ") if matches else []
        fields: list[str] = strip_outer_parentheses(matches.groups()[1]) if matches else []

        derived_fields: dict[str, str] = {
            name_part: function_part
            for function_part, name_part in (field.rsplit(" AS ", 1) for field in fields if " AS " in field)
        }
        pattern = r"([\w\d\_\-]*\#\d*)"
        self.derived_fields = {key: list(set(re.findall(pattern, d))) for key, d in derived_fields.items()}

        self.fields = [field if " AS " not in field else field.rsplit(" AS ", 1)[1] for field in fields]

    def __str__(self) -> str:
        """Return a string representation of the Aggregate node."""
        return f"{self.node_type} (Level: {self.level}) (Grouping: {self.grouping}, Fields: {self.fields}, Derived Fields: {self.derived_fields})"


class Project(PlanNode):
    def __init__(self, node_type: str, level: int, parameters: str) -> None:
        """Initialize a Project instance."""
        super().__init__(node_type, level, parameters)
        self.fields = self.parameters[1:-1].split(", ")

    def __str__(self) -> str:
        """Return a string representation of the LogicalRDD node."""
        return f"{self.node_type} (Level: {self.level}) (Fields: {self.fields})"


class LogicalRDD(PlanNode):
    def __init__(self, node_type: str, level: int, parameters: str) -> None:
        """Initialize a LogicalRDD instance."""
        super().__init__(node_type, level, parameters)
        pattern = r"\[(.*)\],\s.*"
        matches = re.match(pattern, parameters)
        self.fields = matches.groups()[0].split(", ") if matches else []

    def __str__(self) -> str:
        """Return a string representation of the LogicalRDD node."""
        return f"{self.node_type} (Level: {self.level}) (Fields: {self.fields})"


class_registry = {
    "Aggregate": Aggregate,
    "Project": Project,
    "LogicalRDD": LogicalRDD,
}


def _convert_line(line: str) -> PlanNode:
    pattern = r"(\s*)(\+-)?\s*(\w*)\s*(.*)"
    matches = re.match(pattern, line)

    level = int(len(matches.groups()[0]) / 3) + (1 if matches.groups()[1] == "+-" else 0)
    node_type: str = matches.groups()[2]
    parameters: str = matches.groups()[3]

    if node_type in class_registry:
        return class_registry[node_type](node_type, level, parameters)
    return GenericNode(node_type, level, parameters)


def _convert_lines(lines: list[str]) -> PlanNode:
    level_node: dict[int, PlanNode] = {}

    for line in lines:
        converted_node = _convert_line(line)

        level_node[converted_node.level] = converted_node

        if converted_node.level > 0:
            level_node[converted_node.level - 1].add_child(converted_node)

    return level_node[0]


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

    def get_parsed_logical_plan(self) -> PlanNode:
        plan_lines = self._get_section(self.PARSED_LOGICAL_PLAN, self.ANALYZED_LOGICAL_PLAN)
        return _convert_lines(plan_lines)

    def get_analyzed_logical_plan(self) -> PlanNode:
        plan_lines = self._get_section(self.ANALYZED_LOGICAL_PLAN, self.OPTIMIZED_LOGICAL_PLAN, shift=1)
        return _convert_lines(plan_lines)

    def get_optimized_logical_plan(self) -> PlanNode:
        plan_lines = self._get_section(self.OPTIMIZED_LOGICAL_PLAN, self.PHYSICAL_PLAN)
        return _convert_lines(plan_lines)

    def get_physical_plan(self) -> PlanNode:
        plan_lines = self._get_section(self.PHYSICAL_PLAN, None)
        return _convert_lines(plan_lines)
