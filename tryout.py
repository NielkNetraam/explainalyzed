from pathlib import Path

from ea.explainanalyzed import ExplainAnalyzed
from ea.lineage import Lineage

path = Path(__file__).parent / "data/sample/plans"
plans = {p.stem: p for p in path.glob("**/*.txt")}
print(plans)

eas: dict[str, ExplainAnalyzed] = {}
for name, plan in plans.items():
    print(f"Plan name: {name}")
    with plan.open() as file:
        plan_data = file.readlines()

    eas[name] = ExplainAnalyzed(name, plan_data)

lineage = eas["df_joined"].get_lineage()
# print(lineage.mermaid())


print(
    Lineage.mermaid_from_lineages([ea.get_lineage() for ea in eas.values()]),
)

path = Path(__file__).parent / "data/sample/mermaid/sample.mmd"
with path.open("w") as file:
    file.write(Lineage.mermaid_from_lineages([ea.get_lineage() for ea in eas.values()]))

# path = Path(__file__).parent.parent.parent.parent / f"data/plans/{dataset}_plan.txt"
#     with path.open() as file:
#         plan_data = file.readlines()

#     ea = ExplainAnalyzed("target", plan_data)

#     lineage = ea.get_lineage()

# with path.open() as file:
#     plan_d


#     ata = file.readlines()

# ea = ExplainAnalyzed("simple_join_inner", plan_data)

# tree = ea.get_optimized_logical_plan()
# tree.print_node()

# # def print_lineage(lineage: dict[str, Lineage]) -> None:
# #     for name, line in lineage.items():
# #         print(f"{name}: {line}")


# print(" ------- ")
# lineage = ea.get_lineage()
# for tl in lineage.table_lineage:
#     print(tl)
# print(" ------- ")
# for cl in lineage.column_lineage:
#     print(cl)
# print(" ------- ")
# for cl in lineage.source_lineage("sample_table", "name"):
#     print(cl)
# print(" ------- ")
# for cl in lineage.target_lineage("simple_join_inner", "name"):
#     print(cl)

# print
# print(lineage.mermaid())


# path = Path(__file__).parent / "data/mermaid/simple_join_inner.mmd"
# with path.open("w") as file:
#     file.write(lineage.mermaid())

# def print_step(step: PlanStep, indent: int = 0) -> None:
#     print("   " * indent + f"{step.step_type}: {step.parameters}")
#     for child in step.children:
#         print_step(child, indent + 1)


# step.print_step()
# # for step in steps:
# #     print(f"Level: {step.level}, Type: {step.step_type}")  # , Line: {step.line}")
# #     print(step)


# x = "main_peer_group#10, np_or_org#11, reference_dt#15, CASE WHEN (, 0) ELSE [null,null,null] END AS pg_f2#24"
# x = "CASE WHEN ((array_position(array((((np_or_org#11 = NP) AND (reference_dt#15 >= 2025-12-20)) AND (reference_dt#15 < 9999-12-31)), (((np_or_org#11 = NP) AND (reference_dt#15 >= 2019-01-01)) AND (reference_dt#15 < 2025-12-20)), (((np_or_org#11 = ORG) AND (reference_dt#15 >= 2025-12-20)) AND (reference_dt#15 < 9999-12-31)), (((np_or_org#11 = ORG) AND (reference_dt#15 >= 2019-01-01)) AND (reference_dt#15 < 2025-12-20))), true) - 1) >= 0) THEN cast(struct(col1, approx_percentile(f2#13, [0.55,0.5,0.9,0.8], 10000, 0, 0)[(array_position(array((((np_or_org#11 = NP) AND (reference_dt#15 >= 2025-12-20)) AND (reference_dt#15 < 9999-12-31)), (((np_or_org#11 = NP) AND (reference_dt#15 >= 2019-01-01)) AND (reference_dt#15 < 2025-12-20)), (((np_or_org#11 = ORG) AND (reference_dt#15 >= 2025-12-20)) AND (reference_dt#15 < 9999-12-31)), (((np_or_org#11 = ORG) AND (reference_dt#15 >= 2019-01-01)) AND (reference_dt#15 < 2025-12-20))), true) - 1)], col2, [0.550000,0.500000,0.900000,0.800000][(array_position(array((((np_or_org#11 = NP) AND (reference_dt#15 >= 2025-12-20)) AND (reference_dt#15 < 9999-12-31)), (((np_or_org#11 = NP) AND (reference_dt#15 >= 2019-01-01)) AND (reference_dt#15 < 2025-12-20)), (((np_or_org#11 = ORG) AND (reference_dt#15 >= 2025-12-20)) AND (reference_dt#15 < 9999-12-31)), (((np_or_org#11 = ORG) AND (reference_dt#15 >= 2019-01-01)) AND (reference_dt#15 < 2025-12-20))), true) - 1)], col3, [30000.00,30000.00,30000.00,30000.00][(array_position(array((((np_or_org#11 = NP) AND (reference_dt#15 >= 2025-12-20)) AND (reference_dt#15 < 9999-12-31)), (((np_or_org#11 = NP) AND (reference_dt#15 >= 2019-01-01)) AND (reference_dt#15 < 2025-12-20)), (((np_or_org#11 = ORG) AND (reference_dt#15 >= 2025-12-20)) AND (reference_dt#15 < 9999-12-31)), (((np_or_org#11 = ORG) AND (reference_dt#15 >= 2019-01-01)) AND (reference_dt#15 < 2025-12-20))), true) - 1)]) as struct<feature_value:decimal(18,2),param_percentile:decimal(18,6),materiality_threshold:decimal(18,2)>) ELSE [null,null,null] END AS pg_f2#24"
# # x = "Item1, Item2 (Sub1, Sub2), Item3"
# d_fields = strip_outer_parentheses(x)

# # pattern = r",(?![^(]*\))"
# # fileds = re.split(pattern, x)
# # print(len(d_fields), d_fields)
# # print(d_fields[3])

# pattern = r"([\w\d\_\-]*\#\d*)"
# matches = list(set(re.findall(pattern, x)))
# # /gr = matches.groups() if matches else []
# print(len(matches), matches)
# # print(replace_within_parentheses(x))
