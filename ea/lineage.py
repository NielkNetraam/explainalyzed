from dataclasses import dataclass, field
from enum import Enum


class TableType(Enum):
    SOURCE = 1
    TARGET = 2


class ColumnLineageType(Enum):
    COLUMN = 1
    FILTER = 2
    JOIN = 3
    GROUP_BY = 4


@dataclass(frozen=True)
class Table:
    name: str
    type: TableType

    def __str__(self) -> str:
        """Return a string representation of the Column."""
        return f"{self.name}"


@dataclass(frozen=True)
class Column:
    name: str
    table: Table

    def __str__(self) -> str:
        """Return a string representation of the Column."""
        return f"{self.table.name}.{self.name}"


@dataclass(frozen=True)
class TableLineage:
    source_table: Table
    target_table: Table

    def __str__(self) -> str:
        """Return a string representation of the TableLineage."""
        return f"{self.source_table} --> {self.target_table}"


@dataclass(frozen=True)
class ColumnLineage:
    source_column: Column
    target_column: Column
    type: ColumnLineageType

    def __str__(self) -> str:
        """Return a string representation of the ColumnLineage."""
        return f"{self.source_column} -- {self.type.name} --> {self.target_column}"


@dataclass
class Lineage:
    tables: set[Table] = field(default_factory=dict)
    source_columns: set[Column] = field(default_factory=dict)
    target_columns: set[Column] = field(default_factory=dict)
    table_lineage: set[TableLineage] = field(default_factory=set)
    column_lineage: set[ColumnLineage] = field(default_factory=set)

    def source_lineage(self, table_name: str, column_name: str) -> set[ColumnLineage]:
        return {
            cl
            for cl in self.column_lineage
            if cl.source_column.table.name == table_name and cl.source_column.name == column_name
        }

    def target_lineage(self, table_name: str, column_name: str) -> set[ColumnLineage]:
        return {
            cl
            for cl in self.column_lineage
            if cl.target_column.table.name == table_name and cl.target_column.name == column_name
        }

    def _compress_column_lineage(self) -> dict[Column, dict[Column, set[ColumnLineageType]]]:
        ccl: dict[Column, dict[Column, set[ColumnLineageType]]] = {}
        for cl in self.column_lineage:
            if cl.source_column in ccl:
                if cl.target_column in ccl[cl.source_column]:
                    ccl[cl.source_column][cl.target_column].add(cl.type)
                else:
                    ccl[cl.source_column][cl.target_column] = {cl.type}
            else:
                ccl[cl.source_column] = {cl.target_column: {cl.type}}

        return ccl

    def mermaid(self) -> str:
        mermaid_str = (
            "%%{init: {\n"
            "    'theme': 'base',\n"
            "    'themeVariables': {\n"
            "        'primaryColor': '#ffcccc',\n"
            "        'edgeLabelBackground':'#ffffff',\n"
            "        'tertiaryColor': '#fff'\n"
            "    }\n"
            "}}%%\n"
            "flowchart LR\n"
            "    classDef SOURCE stroke:#00f,fill:#0ff,color:black\n"
            "    classDef TARGET stroke:#0f0,fill:#0fa,color:black\n"
            "\n"
        )

        for table in self.tables:
            mermaid_str += f"    {table.name}:::{table.type.name}\n"

        for table in sorted(self.tables, key=lambda t: t.name):
            mermaid_str += f"\n    subgraph {table.name}\n"

            if table.type == TableType.SOURCE:
                for column in sorted(self.source_columns, key=lambda t: t.name):
                    if column.table.name == table.name:
                        mermaid_str += f"        {table.name}.{column.name}[{column.name}]\n"

            if table.type == TableType.TARGET:
                for column in sorted(self.target_columns, key=lambda t: t.name):
                    if column.table.name == table.name:
                        mermaid_str += f"        {table.name}.{column.name}[{column.name}]\n"

            mermaid_str += "    end\n"

        mermaid_str += "\n"

        compressed_colum_lineage = self._compress_column_lineage()
        for sc, t in sorted(compressed_colum_lineage.items(), key=lambda t: str(t[0])):
            for tc, types in sorted(t.items(), key=lambda t: str(t[0])):
                mermaid_str += f"    {sc} -- {','.join(sorted({t.name for t in types}))} --> {tc}\n"

        return mermaid_str
