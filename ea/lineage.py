from dataclasses import dataclass, field
from enum import Enum


class TableType(Enum):
    SOURCE = 1
    INTERMEDIATE = 2
    TARGET = 3
    INTERNAL = 4


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

    def __hash__(self) -> int:
        """Return hash of the object based on table name and attribute name."""
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        """Check equality based on table name."""
        if not isinstance(other, Table):
            return NotImplemented
        return self.name == other.name

    def __lt__(self, other: "Table") -> bool:
        """Compare tables based on their names for sorting."""
        return self.type.value < other.type.value or (self.type == other.type and self.name < other.name)


@dataclass(frozen=True)
class Column:
    name: str
    table: Table

    def __str__(self) -> str:
        """Return a string representation of the Column."""
        return f"{self.table.name}.{self.name}"

    def __hash__(self) -> int:
        """Return hash of the object based on table name and attribute name."""
        return hash((self.table.name, self.name))

    def __eq__(self, other: object) -> bool:
        """Check equality based on table name and attribute name."""
        if not isinstance(other, Column):
            return NotImplemented
        return self.table.name == other.table.name and self.name == other.name


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

    @staticmethod
    def _compress_column_lineage(column_lineage: set[ColumnLineage]) -> dict[Column, dict[Column, set[ColumnLineageType]]]:
        ccl: dict[Column, dict[Column, set[ColumnLineageType]]] = {}
        for cl in column_lineage:
            if cl.source_column in ccl:
                if cl.target_column in ccl[cl.source_column]:
                    ccl[cl.source_column][cl.target_column].add(cl.type)
                else:
                    ccl[cl.source_column][cl.target_column] = {cl.type}
            else:
                ccl[cl.source_column] = {cl.target_column: {cl.type}}

        return ccl

    def mermaid(self) -> str:
        compressed_column_lineage = Lineage._compress_column_lineage(self.column_lineage)
        columns = self.source_columns.union(self.target_columns)
        return Lineage._mermaid(self.tables, columns, compressed_column_lineage)

    @staticmethod
    def _mermaid(
        tables: set[Table],
        columns: set[Column],
        compressed_column_lineage: dict[Column, dict[Column, set[ColumnLineageType]]],
    ) -> str:
        def table_columns(columns: set[Column]) -> str:
            mermaid_str = ""
            for column in sorted(columns, key=lambda t: t.name):
                if column.table.name == table.name:
                    mermaid_str += f"        {table.name}.{column.name}[{column.name}]\n"

            return mermaid_str

        mermaid_str = (
            "---\n"
            "config:\n"
            "theme: 'base'\n"
            "themeVariables:\n"
            "    primaryColor: '#BB2528'\n"
            "    primaryTextColor: '#fff'\n"
            "    primaryBorderColor: '#7C0000'\n"
            "    lineColor: '#F8B229'\n"
            "    secondaryColor: '#267826'\n"
            "    tertiaryColor: '#fff'\n"
            "---\n"
            "flowchart TD\n"
            "    classDef SOURCE stroke:Blue,fill:LightBlue,color:black\n"
            "    classDef TARGET stroke:Green,fill:LightGreen,color:black\n"
            "    classDef INTERMEDIATE stroke:DarkOrange,fill:Orange,color:black\n"
            "    classDef INTERNAL stroke:Purple,fill:Violet,color:black\n"
            "\n"
        )

        for table in sorted(tables):
            mermaid_str += f"    {table.name}:::{table.type.name}\n"

        for table in sorted(tables):
            mermaid_str += f"\n    subgraph {table.name}\n"
            mermaid_str += table_columns(columns)
            mermaid_str += "    end\n"

        mermaid_str += "\n"

        for sc, t in sorted(compressed_column_lineage.items(), key=lambda t: str(t[0])):
            for tc, types in sorted(t.items(), key=lambda t: str(t[0])):
                mermaid_str += f"    {sc} -- {','.join(sorted({t.name for t in types}))} --> {tc}\n"

        return mermaid_str

    @staticmethod
    def mermaid_from_lineages(lineages: list["Lineage"]) -> str:
        table_types: dict[str, TableType] = {}
        columns: set[Column] = set()
        compressed_column_lineage: dict[Column, dict[Column, set[ColumnLineageType]]] = {}
        for lineage in lineages:
            for table in lineage.tables:
                if table.name not in table_types:
                    table_types[table.name] = table.type
                elif table_types[table.name] != table.type:
                    table_types[table.name] = TableType.INTERMEDIATE

            _columns = {c for c in lineage.source_columns.union(lineage.target_columns) if c not in columns}
            columns = columns.union(_columns)
            compressed_column_lineage = compressed_column_lineage | Lineage._compress_column_lineage(lineage.column_lineage)

        tables: set[Table] = {Table(name=table_name, type=table_type) for table_name, table_type in table_types.items()}

        return Lineage._mermaid(tables, columns, compressed_column_lineage)
