from pathlib import Path

from ea.explainanalyzed import ExplainAnalyzed
from ea.lineage import Lineage


def test_complex_example() -> None:
    parent = Path(__file__).parent.parent.parent.parent
    path = parent / "data/complex_example/plans"
    plans = {p.stem: p for p in path.glob("**/*.txt")}

    eas: dict[str, ExplainAnalyzed] = {}
    for name, plan in plans.items():
        with plan.open() as file:
            plan_data = file.readlines()

        eas[name] = ExplainAnalyzed(name, plan_data)

        path = parent / f"data/complex_example/visualization/{name}.mmd"
        with path.open("w") as file:
            file.write(eas[name].mermaid())

    path = parent / "data/complex_example/lineage/complex_example.mmd"
    with path.open("w") as file:
        file.write(Lineage.mermaid_from_lineages([ea.get_lineage() for ea in eas.values()]))
