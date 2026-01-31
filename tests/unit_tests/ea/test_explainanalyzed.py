from pathlib import Path

import pytest

from ea.explainanalyzed import ExplainAnalyzed


@pytest.fixture
def plan_data() -> list[str]:
    path = Path("data/plan.txt")
    with path.open() as file:
        return file.readlines()


def test_plan_data(plan_data: list[str]) -> None:
    ea = ExplainAnalyzed(plan_data)
    assert len(ea.plan_data) > 0
    assert "== Parsed Logical Plan ==" in ea.get_parsed_logical_plan()
