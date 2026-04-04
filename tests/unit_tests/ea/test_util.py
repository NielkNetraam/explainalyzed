import pytest

from ea.util import findall_column_ids, get_dependencies, split_field

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
    "agg_pivot_number": ("pivotfirst(id#253, sum(age)#264L, 1, 2, 3, 0, 0) AS __pivot_sum(age)", {"id#253", "sum(age)#264l"}),
    "case": ("CASE WHEN (category#401483 = 2) THEN count#401484 ELSE 0 END", {"category#401483", "count#401484"}),
    "case_with_function": (
        "CASE WHEN (category#401483 = 2) THEN cast((cast(count#401484 as double) * map(keys: [0.01,0.02,0.05], "
        "values: [0.01,0.02,0.05])[cast(value#401485 as string)]) as decimal(18,2)) ELSE 0.00 END",
        {"category#401483", "count#401484", "value#401485"},
    ),
}
# [beb_key#401482, count#401484, value#401485, new AS year#406299, CASE WHEN (category#401483 = 2) THEN count#401484 ELSE 0 END AS counterfeit_count#406506, CASE WHEN (category#401483 = 2) THEN cast((cast(count#401484 as double) * map(keys: [0.01,0.02,0.05,0.10,0.20,0.50,1.00,2.00,5.00,10.00,20.00,50.00,100.00,200.00,500.00], values: [0.01,0.02,0.05,0.1,0.2,0.5,1.0,2.0,5.0,10.0,20.0,50.0,100.0,200.0,500.0])[cast(value#401485 as string)]) as decimal(18,2)) ELSE 0.00 END AS counterfeit_value#406513]


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
    "literal_str": ("new AS age#617", ("age#617", {"__literal__"})),
    "case": (
        "CASE WHEN (category#401483 = 2) THEN count#401484 ELSE 0 END AS counterfeit_count#406506",
        ("counterfeit_count#406506", {"category#401483", "count#401484"}),
    ),
    "case_with_function": (
        "CASE WHEN (category#401483 = 2) THEN cast((cast(count#401484 as double) * map(keys: [0.01,0.02,0.05], "
        "values: [0.01,0.02,0.05])[cast(value#401485 as string)]) as decimal(18,2)) ELSE 0.00 END AS counterfeit_value#406513",
        (
            "counterfeit_value#406513",
            {"category#401483", "count#401484", "value#401485"},
        ),
    ),
}


@pytest.mark.parametrize(
    ("field", "expected"),
    split_field_scenarios.values(),
    ids=split_field_scenarios.keys(),
)
def test_split_field(field: str, expected: tuple[str, set[str]]) -> None:
    assert split_field(field) == expected


findall_column_ids_scenarios = {
    "complex": (
        "(size(transform(names#727, lambdafunction(CASE WHEN ((lambda y_6#737 % 2) = 0) "
        "THEN lambda x_5#736 ELSE upper(lambda x_5#736) END, lambda x_5#736, lambda y_6#737, false)), false) > 0)",
        {"names#727"},
    ),
}


@pytest.mark.parametrize(("line", "expected"), findall_column_ids_scenarios.values(), ids=findall_column_ids_scenarios.keys())
def test_findall_column_ids(line: str, expected: set[str]) -> None:
    assert set(findall_column_ids(line)) == expected
