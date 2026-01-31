import pytest

from ea.explainanalyzed import _split_line

_split_line_scenarios = {
    "IF level 0 THEN correct line": (
        "Aggregate [avg(age#88) AS avg_age#89, max(age#88) AS max_age#90]",
        (0, None, "Aggregate", "[avg(age#88) AS avg_age#89, max(age#88) AS max_age#90]"),
    ),
    "IF level 1+- THEN correct line": ("+- Project [age#88]", (1, None, "Project", "[age#88]")),
    "IF level 2+- THEN correct line": (
        "   +- Relation [id#86,name#87,age#88] parquet",
        (2, None, "Relation", "[id#86,name#87,age#88] parquet"),
    ),
    "IF level 2:- THEN correct line": ("   :- Project [name#61, id2#67]", (2, None, "Project", "[name#61, id2#67]")),
    "IF level 3:+- THEN correct line": ("   :  +- Join Inner, (id#60 = id1#66)", (3, None, "Join", "Inner, (id#60 = id1#66)")),
    "IF level 4::- THEN correct line": ("   :     :- Union false, false", (4, None, "Union", "false, false")),
    "IF level 5::- THEN correct line": ("   :     :  :- Project [id#60, name#61]", (5, None, "Project", "[id#60, name#61]")),
    "IF level 6:::+- THEN correct line": ("   :     :  :  +- Filter isnotnull(id#60)", (6, None, "Filter", "isnotnull(id#60)")),
    "IF level 5::+- THEN correct line": ("   :     :  +- Project [id#63, name#64]", (5, None, "Project", "[id#63, name#64]")),
    "IF level 4:+- THEN correct line": ("   :     +- Project [id1#66, id2#67]", (4, None, "Project", "[id1#66, id2#67]")),
}


@pytest.mark.parametrize(("line", "expected"), _split_line_scenarios.values(), ids=_split_line_scenarios.keys())
def test__split_line(line: str, expected: tuple[int, str | None, str, str]) -> None:
    assert _split_line(line) == expected


# @pytest.fixture
# def plan_data() -> list[str]:
#     path = Path("data/plan.txt")
#     with path.open() as file:
#         return file.readlines()


# def test_plan_data(plan_data: list[str]) -> None:
#     ea = ExplainAnalyzed(plan_data)
#     assert len(ea.plan_data) > 0
#     assert "== Parsed Logical Plan ==" in ea.get_parsed_logical_plan()
