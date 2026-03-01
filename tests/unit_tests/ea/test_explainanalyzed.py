from pathlib import Path

import pytest

from ea.explainanalyzed import ExplainAnalyzed, _split_line

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


_get_lineage_scenarios = {
    "IF select_1 THEN success": (
        "select_1",
        {
            "sample_table.id -- COLUMN --> target.id",
            "sample_table.name -- COLUMN --> target.name",
            "sample_table.age -- COLUMN --> target.age",
        },
    ),
    "IF select_2 THEN success": (
        "select_2",
        {
            "sample_table.id -- COLUMN --> target.id",
            "sample_table.name -- COLUMN --> target.name",
        },
    ),
    "IF filter THEN success": (
        "filter",
        {
            "sample_table.id -- COLUMN --> target.id",
            "sample_table.name -- COLUMN --> target.name",
            "sample_table.age -- COLUMN --> target.age",
            "sample_table.id -- FILTER --> target.id",
            "sample_table.id -- FILTER --> target.name",
            "sample_table.id -- FILTER --> target.age",
        },
    ),
    "IF filter_not_in_output THEN success": (
        "filter_not_in_output",
        {
            "sample_table.name -- COLUMN --> target.name",
            "sample_table.id -- FILTER --> target.name",
        },
    ),
    "IF derive THEN success": (
        "derive",
        {
            "sample_table.id -- COLUMN --> target.id",
            "sample_table.name -- COLUMN --> target.original_name",
            "sample_table.age -- COLUMN --> target.age",
            "sample_table.name -- COLUMN --> target.name_upper",
            "sample_table.id -- COLUMN --> target.id_str",
            "sample_table.id -- COLUMN --> target.id_and_name",
            "sample_table.name -- COLUMN --> target.id_and_name",
            "internal.literal -- COLUMN --> target.literal",
            "internal.None -- COLUMN --> target.none_value",
            "sample_table.id -- COLUMN --> target.conditional_value",
            "sample_table.name -- COLUMN --> target.conditional_value",
            "sample_table.name -- COLUMN --> target.name_age",
            "sample_table.age -- COLUMN --> target.name_age",
        },
    ),
    "IF union THEN success": (
        "union",
        {
            "sample_table.name -- COLUMN --> target.name",
            "sample_table.age -- COLUMN --> target.age",
            "internal.None -- COLUMN --> target.birth_date",
            "sample_table_2.name -- COLUMN --> target.name",
            "internal.None -- COLUMN --> target.age",
            "sample_table_2.birth_date -- COLUMN --> target.birth_date",
        },
    ),
    "IF join_inner THEN success": (
        "join_inner",
        {
            "sample_table.id -- COLUMN --> target.id",
            "sample_table.id -- JOIN --> target.id",
            "sample_table.id -- FILTER --> target.id",
            "sample_table_2.id -- JOIN --> target.id",
            "sample_table.name -- COLUMN --> target.name",
            "sample_table.id -- JOIN --> target.name",
            "sample_table.id -- FILTER --> target.name",
            "sample_table_2.name -- COLUMN --> target.name",
            "sample_table_2.id -- JOIN --> target.name",
            "sample_table_2.id -- FILTER --> target.name",
            "sample_table.age -- COLUMN --> target.age",
            "sample_table.id -- JOIN --> target.age",
            "sample_table.id -- FILTER --> target.age",
            "sample_table_2.id -- JOIN --> target.age",
            "sample_table_2.birth_date -- COLUMN --> target.birth_date",
            "sample_table.id -- JOIN --> target.birth_date",
            "sample_table_2.id -- JOIN --> target.birth_date",
            "sample_table_2.id -- FILTER --> target.birth_date",
        },
    ),
    "IF join_left THEN success": (
        "join_left",
        {
            "sample_table.id -- COLUMN --> target.id",
            "sample_table.id -- JOIN --> target.id",
            "sample_table_2.id -- JOIN --> target.id",
            "sample_table.name -- COLUMN --> target.name",
            "sample_table.id -- JOIN --> target.name",
            "sample_table_2.name -- COLUMN --> target.name",
            "sample_table_2.id -- JOIN --> target.name",
            "sample_table_2.id -- FILTER --> target.name",
            "sample_table.age -- COLUMN --> target.age",
            "sample_table.id -- JOIN --> target.age",
            "sample_table_2.id -- JOIN --> target.age",
            "sample_table_2.birth_date -- COLUMN --> target.birth_date",
            "sample_table.id -- JOIN --> target.birth_date",
            "sample_table_2.id -- JOIN --> target.birth_date",
            "sample_table_2.id -- FILTER --> target.birth_date",
        },
    ),
    "IF join_right THEN success": (
        "join_right",
        {
            "sample_table_2.id -- COLUMN --> target.id",
            "sample_table.id -- JOIN --> target.id",
            "sample_table_2.id -- JOIN --> target.id",
            "sample_table.name -- COLUMN --> target.name",
            "sample_table.id -- JOIN --> target.name",
            "sample_table.id -- FILTER --> target.name",
            "sample_table_2.name -- COLUMN --> target.name",
            "sample_table_2.id -- JOIN --> target.name",
            "sample_table.age -- COLUMN --> target.age",
            "sample_table.id -- JOIN --> target.age",
            "sample_table.id -- FILTER --> target.age",
            "sample_table_2.id -- JOIN --> target.age",
            "sample_table_2.birth_date -- COLUMN --> target.birth_date",
            "sample_table.id -- JOIN --> target.birth_date",
            "sample_table_2.id -- JOIN --> target.birth_date",
        },
    ),
    "IF join_2 THEN success": (
        "join_2",
        {
            "sample_table.id -- JOIN --> target.name_b",
            "sample_table.id -- FILTER --> target.name_a",
            "sample_table.name -- COLUMN --> target.name_a",
            "sample_table.name -- COLUMN --> target.name_b",
            "sample_table.id -- FILTER --> target.name_b",
            "sample_table.id -- JOIN --> target.name_a",
            "sample_table_2.id -- FILTER --> target.name_b",
            "sample_table_2.name -- COLUMN --> target.name_b",
            "sample_table_2.name -- COLUMN --> target.name_a",
            "sample_table_2.id -- FILTER --> target.name_a",
            "sample_table_2.id -- JOIN --> target.name_a",
            "sample_table_2.id -- JOIN --> target.name_b",
            "relation_table.id1 -- JOIN --> target.name_a",
            "relation_table.id1 -- JOIN --> target.name_b",
            "relation_table.id2 -- JOIN --> target.name_a",
            "relation_table.id2 -- JOIN --> target.name_b",
            "relation_table.rel_type -- JOIN --> target.name_a",
            "relation_table.rel_type -- JOIN --> target.name_b",
        },
    ),
    "IF join_2_broadcast THEN success": (
        "join_2_broadcast",
        {
            "sample_table.id -- JOIN --> target.name_b",
            "sample_table.id -- FILTER --> target.name_a",
            "sample_table.name -- COLUMN --> target.name_a",
            "sample_table.name -- COLUMN --> target.name_b",
            "sample_table.id -- FILTER --> target.name_b",
            "sample_table.id -- JOIN --> target.name_a",
            "sample_table_2.id -- FILTER --> target.name_b",
            "sample_table_2.name -- COLUMN --> target.name_b",
            "sample_table_2.name -- COLUMN --> target.name_a",
            "sample_table_2.id -- FILTER --> target.name_a",
            "sample_table_2.id -- JOIN --> target.name_a",
            "sample_table_2.id -- JOIN --> target.name_b",
            "relation_table.id1 -- JOIN --> target.name_a",
            "relation_table.id1 -- JOIN --> target.name_b",
            "relation_table.id2 -- JOIN --> target.name_a",
            "relation_table.id2 -- JOIN --> target.name_b",
            "relation_table.rel_type -- JOIN --> target.name_a",
            "relation_table.rel_type -- JOIN --> target.name_b",
        },
    ),
    "IF aggregation_1 THEN success": (
        "aggregation_1",
        {
            "sample_table.age -- COLUMN --> target.avg_age",
            "sample_table.age -- COLUMN --> target.max_age",
        },
    ),
    "IF aggregation_2 THEN success": (
        "aggregation_2",
        {
            "sample_table.age -- COLUMN --> target.age",
            "sample_table.age -- GROUP_BY --> target.age",
            "sample_table.age -- GROUP_BY --> target.count",
            "sample_table.name -- COLUMN --> target.max_name",
            "sample_table.age -- GROUP_BY --> target.max_name",
        },
    ),
    "IF aggregation_3 THEN success": (
        "aggregation_3",
        {
            "sample_table.id -- COLUMN --> target.id",
            "sample_table.id -- JOIN --> target.id",
            "sample_table.id -- GROUP_BY --> target.id",
            "transaction_table.id -- GROUP_BY --> target.id",
            "transaction_table.id -- JOIN --> target.id",
            "sample_table.id -- GROUP_BY --> target.count",
            "transaction_table.id -- GROUP_BY --> target.count",
            "transaction_table.sign -- COLUMN --> target.total_amount",
            "transaction_table.amount -- COLUMN --> target.total_amount",
            "sample_table.id -- JOIN --> target.total_amount",
            "sample_table.id -- GROUP_BY --> target.total_amount",
            "transaction_table.id -- GROUP_BY --> target.total_amount",
            "transaction_table.id -- FILTER --> target.total_amount",
            "transaction_table.id -- JOIN --> target.total_amount",
        },
    ),
    "IF union_forest THEN success": (
        "union_forest",
        {
            "sample_table.id -- JOIN --> target.simple_id",
            "sample_table.id -- COLUMN --> target.simple_id",
            "sample_table.name -- FILTER --> target.simple_id",
            "sample_table.id -- FILTER --> target.name_b",
            "sample_table.name -- JOIN --> target.name_a",
            "sample_table.name -- JOIN --> target.simple_id",
            "sample_table_2.id -- JOIN --> target.name_a",
            "sample_table.id -- FILTER --> target.name_a",
            "sample_table_2.name -- FILTER --> target.name_a",
            "relation_table.rel_type -- JOIN --> target.name_a",
            "sample_table.name -- FILTER --> target.complex_id",
            "internal.None -- COLUMN --> target.complex_id",
            "relation_table.id2 -- JOIN --> target.complex_id",
            "relation_table.id1 -- JOIN --> target.simple_id",
            "sample_table_2.name -- JOIN --> target.name_b",
            "sample_table_2.name -- COLUMN --> target.name_a",
            "relation_table.id1 -- JOIN --> target.name_b",
            "internal.None -- COLUMN --> target.simple_id",
            "sample_table_2.name -- JOIN --> target.name_a",
            "relation_table.id2 -- JOIN --> target.name_b",
            "sample_table_2.name -- JOIN --> target.simple_id",
            "relation_table.rel_type -- JOIN --> target.name_b",
            "sample_table_2.id -- FILTER --> target.name_b",
            "relation_table.rel_type -- JOIN --> target.complex_id",
            "sample_table_2.id -- FILTER --> target.name_a",
            "sample_table_2.id -- JOIN --> target.simple_id",
            "sample_table_2.id -- JOIN --> target.name_b",
            "sample_table.id -- JOIN --> target.complex_id",
            "relation_table.id2 -- JOIN --> target.name_a",
            "sample_table_2.name -- COLUMN --> target.name_b",
            "relation_table.id2 -- JOIN --> target.simple_id",
            "relation_table.id1 -- JOIN --> target.name_a",
            "sample_table.id -- JOIN --> target.name_b",
            "relation_table.rel_type -- JOIN --> target.simple_id",
            "relation_table.id1 -- JOIN --> target.complex_id",
            "sample_table.name -- FILTER --> target.name_a",
            "sample_table.id -- JOIN --> target.name_a",
            "sample_table_2.name -- JOIN --> target.complex_id",
            "sample_table.name -- COLUMN --> target.name_b",
            "sample_table_2.id -- JOIN --> target.complex_id",
            "sample_table.name -- JOIN --> target.complex_id",
            "sample_table.id -- COLUMN --> target.complex_id",
            "sample_table.name -- COLUMN --> target.name_a",
            "sample_table.name -- JOIN --> target.name_b",
        },
    ),
}


@pytest.mark.parametrize(("dataset", "expected"), _get_lineage_scenarios.values(), ids=_get_lineage_scenarios.keys())
def test_get_lineage(dataset: str, expected: set[str]) -> None:
    path = Path(__file__).parent.parent.parent.parent / f"data/plans/{dataset}_plan.txt"
    with path.open() as file:
        plan_data = file.readlines()

    ea = ExplainAnalyzed("target", plan_data)

    lineage = ea.get_lineage()

    assert {str(cl) for cl in lineage.column_lineage} == expected

    path = Path(__file__).parent.parent.parent.parent / f"data/lineage/{dataset}.mmd"
    with path.open("w") as file:
        file.write(lineage.mermaid())

    path = Path(__file__).parent.parent.parent.parent / f"data/visualization/{dataset}.mmd"
    with path.open("w") as file:
        file.write(ea.mermaid())
