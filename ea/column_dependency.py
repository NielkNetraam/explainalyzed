import re
from abc import ABC, abstractmethod

from ea.lineage import Column, ColumnLineage, ColumnLineageType, Table, TableType


class ColumnDependency(ABC):
    def __init__(self, column_id: str) -> None:
        self.column_id = column_id
        self.column_name = column_id.split("#")[0] if "#" in column_id else column_id

    def __str__(self) -> str:
        """Return a string representation of the ColumnDependency."""
        return f"{self.column_id}"

    @abstractmethod
    def get_column_lineage(self, dataset_name: str | None = None) -> set[ColumnLineage]:
        pass


class SourceColumnDependency(ColumnDependency):
    def __init__(self, column_id: str, location: list[str]) -> None:
        super().__init__(column_id)
        self.location: list[str] = location
        self.table: str | None = self._extract_table_from_location()

    def _extract_table_from_location(self) -> str | None:
        for loc in self.location:
            pattern = r".*/(.*)$"
            matches = re.match(pattern, loc)
            if matches:
                return matches.groups()[0]
        return None

    def __str__(self) -> str:
        """Return a string representation of the SourceColumnDependency."""
        return f"{self.table}.{self.column_name}"

    def get_column_lineage(self, dataset_name: str | None = None) -> set[ColumnLineage]:
        source_table = Table(self.table if self.table else "internal", TableType.SOURCE if self.table else TableType.INTERNAL)
        target_table = Table(dataset_name if dataset_name else "target", TableType.TARGET)
        source_column = Column(self.column_name, source_table)
        target_column = Column(self.column_name, target_table)

        return {ColumnLineage(source_column=source_column, target_column=target_column, type=ColumnLineageType.COLUMN)}


class DerivedColumnDependency(ColumnDependency):
    def __init__(
        self,
        column_id: str,
        columns: list[ColumnDependency] | None = None,
        filters: list[ColumnDependency] | None = None,
        grouping_keys: list[ColumnDependency] | None = None,
        join_keys: list[ColumnDependency] | None = None,
    ) -> None:
        super().__init__(column_id)
        self.columns: list[ColumnDependency] = columns if columns is not None else []
        self.filters: list[ColumnDependency] = filters if filters is not None else []
        self.grouping_keys: list[ColumnDependency] = grouping_keys if grouping_keys is not None else []
        self.join_keys: list[ColumnDependency] = join_keys if join_keys is not None else []

    def __str__(self) -> str:
        """Return a string representation of the DerivedColumnDependency."""
        column_str = ", ".join(str(column) for column in self.columns)
        filter_str = ", ".join(str(flt) for flt in self.filters)
        grouping_key_str = ", ".join(str(flt) for flt in self.grouping_keys)
        join_key_str = ", ".join(str(flt) for flt in self.join_keys)
        return (
            f"{self.column_name} (Fields: [{column_str}]; Filters: [{filter_str}]; "
            f"Grouping keys: [{grouping_key_str}]; Join keys: [{join_key_str}])"
        )

    def get_column_lineage(self, dataset_name: str | None = None) -> set[ColumnLineage]:
        column_lineage: set[ColumnLineage] = set()

        target_table = Table(dataset_name if dataset_name else "target", TableType.TARGET)
        target_column = Column(self.column_name, target_table)

        column_lineage_columns = {
            ColumnLineage(source_column=cl.source_column, target_column=target_column, type=cl.type)
            for col in self.columns
            for cl in col.get_column_lineage(dataset_name)
        }
        column_lineage.update(column_lineage_columns)

        column_lineage_filters = {
            ColumnLineage(source_column=cl.source_column, target_column=target_column, type=ColumnLineageType.FILTER)
            for col in self.filters
            for cl in col.get_column_lineage(dataset_name)
        }
        column_lineage.update(column_lineage_filters)

        column_lineage_join_keys = {
            ColumnLineage(source_column=cl.source_column, target_column=target_column, type=ColumnLineageType.JOIN)
            for col in self.join_keys
            for cl in col.get_column_lineage(dataset_name)
        }
        column_lineage.update(column_lineage_join_keys)

        column_lineage_grouping_keys = {
            ColumnLineage(source_column=cl.source_column, target_column=target_column, type=ColumnLineageType.GROUP_BY)
            for col in self.grouping_keys
            for cl in col.get_column_lineage(dataset_name)
        }
        column_lineage.update(column_lineage_grouping_keys)

        return column_lineage
