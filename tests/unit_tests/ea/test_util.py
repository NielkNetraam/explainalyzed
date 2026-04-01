import pytest

from ea.util import get_dependencies, split_field

get_dependencies_scenarios = {
    "simple": ("age#617", {"age#617"}),
    "simple_minus": ("-age#617", {"age#617"}),
    "multiple": ("age#617, names#618", {"age#617", "names#618"}),
    "surrounded": ("avg(age#617)", {"age#617"}),
    "function": ("avg(age)#617", {"avg(age)#617"}),
    "quoted_function": ("`avg(age)`#617", {"`avg(age)`#617"}),
    "with_lambda": ("lambda x_1#6861", set()),
    "with_lambda_full": ("transform(names#679, lambdafunction(upper(lambda x_1#686), lambda x_1#686, false) )", {"names#679"}),
    "pivot": ("pivotfirst(id#219, avg(age)#230, 1, 2, 3, 0, 0) AS __pivot_avg(age)", {"id#219", "avg(age)#230"}),
}


@pytest.mark.parametrize(
    ("dependency_str", "expected"),
    get_dependencies_scenarios.values(),
    ids=get_dependencies_scenarios.keys(),
)
def test_get_dependencies(dependency_str: str, expected: set[str]) -> None:
    assert get_dependencies(dependency_str) == expected


split_field_scenarios = {
    "simple": ("age#617", ("age#617", {"age#617"})),
    "simple_minus": ("-age#617 AS age#618", ("age#618", {"age#617"})),
    "with_lambda": (
        "transform(names#679, lambdafunction(upper(lambda x_1#686), lambda x_1#686, false)) AS names_upper#685",
        ("names_upper#685", {"names#679"}),
    ),
    "pivot": (
        "pivotfirst(id#219, avg(age)#230, 1, 2, 3, 0, 0) AS __pivot_avg(age) AS `avg(age)`#238",
        ("`avg(age)`#238", {"id#219", "avg(age)#230"}),
    ),
    "null": ("null AS age#617", ("age#617", {"__none__"})),
    "literal": ("1 AS age#617", ("age#617", {"__literal__"})),
}


@pytest.mark.parametrize(
    ("field", "expected"),
    split_field_scenarios.values(),
    ids=split_field_scenarios.keys(),
)
def test_split_field(field: str, expected: tuple[str, set[str]]) -> None:
    assert split_field(field) == expected
