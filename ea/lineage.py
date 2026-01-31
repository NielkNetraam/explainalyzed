import re


class Lineage:
    def __init__(self, column_id: str) -> None:
        self.column_id = column_id
        self.name = column_id.split("#")[0] if "#" in column_id else column_id

    def __str__(self) -> str:
        """Return a string representation of the Lineage."""
        return f"{self.column_id}"


class SourceLineage(Lineage):
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
        """Return a string representation of the SourceLineage."""
        return f"{self.table}.{self.name}"


class DerivedLineage(Lineage):
    def __init__(
        self,
        column_id: str,
        fields: list[Lineage] | None = None,
        filters: list[Lineage] | None = None,
        grouping_keys: list[Lineage] | None = None,
        join_keys: list[Lineage] | None = None,
    ) -> None:
        super().__init__(column_id)
        self.fields: list[Lineage] = fields if fields is not None else []
        self.filters: list[Lineage] = filters if filters is not None else []
        self.grouping_keys: list[Lineage] = grouping_keys if grouping_keys is not None else []
        self.join_keys: list[Lineage] = join_keys if join_keys is not None else []

    def __str__(self) -> str:
        """Return a string representation of the DerivedLineage."""
        field_str = ", ".join(str(field) for field in self.fields)
        filter_str = ", ".join(str(flt) for flt in self.filters)
        grouping_key_str = ", ".join(str(flt) for flt in self.grouping_keys)
        join_key_str = ", ".join(str(flt) for flt in self.join_keys)
        return (
            f"{self.name} (Fields: [{field_str}]; Filters: [{filter_str}]; "
            f"Grouping keys: [{grouping_key_str}]; Join keys: [{join_key_str}])"
        )
