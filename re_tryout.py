# a = {"key1": ["a", "b", "c"], "key2": ["d", "e"], "key3": ["f", "g", "h", "i"]}
# b = {"key3": ["a", "c"], "key4": ["d", "fe"], "key5": ["1f", "1g", "h", "i"]}
# x = zip(a.keys(), b.keys(), strict=False)
# v = zip(a.values(), b.values(), strict=False)
# print([k[0] for k in list(x)])
# print(list(v))
# l1 = "[id#24, name#25, (age#26 + 1) AS age#28, upper(name#25) AS name_upper#29, cast(id#24 as string) AS id_str#30]"


# fields = strip_outer_parentheses(l1[1:-1])

# for f in fields:
#     if " AS " in f:
#         name_part = f.rsplit(" AS ", 1)[1]
#         function_part = f.rsplit(" AS ", 1)[0]

#         pattern = r"([\w\d\_\-]*\#\d*)"
#         src_fields = list(set(re.findall(pattern, function_part)))
#         print(f"Name: {name_part}, Function: {src_fields}")
#     else:
#         print(f"Field: {f}")

# print(fields)

import re

l1 = "[avg(age#61) AS avg_age#62, max(age#61) AS max_age#63]"
l1 = "[id#76], [id#76, count(1) AS count#84L, sum(CASE WHEN (sign#81 = DEBIT) THEN amount#80 ELSE -amount#80 END) AS total_amount#85]"
# l1 = ":  +- *(1) ColumnarToRow"
# l1 = ":     +- FileScan parquet [age#25] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/tisit/project/explainalyzed/data/tables/sample_table], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<age:int>"
# l1 = "+- *(2) Project [null AS age#29, birth_date#28]"
# l1="   +- *(2) ColumnarToRow"
# l1="      +- FileScan parquet [birth_date#28] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/tisit/project/explainalyzed/data/tables/sample_table_2], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<birth_date:date>"

pattern = r"\[(.*)\],\s\[(.*)\]"
# pattern = r"(\s*)(\+-)?\s*(\*\(\d*\))?\s*(\w*)\s*(.*)"
# pattern = r".*/(.*)$"
matches = re.match(pattern, l1)
groups = matches.groups() if matches else ()
for g in groups:
    print(f"'{g}'")


print(re.findall(r"\[([^\[]*)\]", l1))

# l1 = "Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/tisit/project/explainalyzed/ea/data/tables/sample_table], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:int,name:string>"
# p1 = r"Batched: (.*), DataFilters: \[(.*)\], Format: (\w*), Location: .*\[(.*)\], PartitionFilters: \[(.*)\], PushedFilters: \[(.*)\], ReadSchema: (.*)"
# matches = re.match(p1, l1)
# groups = matches.groups() if matches else ()
# for g in groups:
#     print(f"'{g}'")

# a = ""
# print(a.split(", "))
