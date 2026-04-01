import re

from ea.util import split_field, strip_outer_parentheses

# Project [name#220, __pivot_avg(age) AS `avg(age)`#238[0] AS 1#239, __pivot_avg(age) AS `avg(age)`#238[1] AS 2#240, __pivot_avg(age) AS `avg(age)`#238[2] AS 3#241]
# +- Aggregate [name#220], [name#220, pivotfirst(id#219, avg(age)#230, 1, 2, 3, 0, 0) AS __pivot_avg(age) AS `avg(age)`#238]
#    +- Aggregate [name#220, id#219], [name#220, id#219, avg(age#221) AS avg(age)#230]
#       +- Relation [id#219,name#220,age#221] parquet

parameters = "[name#220, __pivot_avg(age) AS `avg(age)`#238[0] AS 1#239, __pivot_avg(age) AS `avg(age)`#238[1] AS 2#240, __pivot_avg(age) AS `avg(age)`#238[2] AS 3#241]"


fields = strip_outer_parentheses(parameters[1:-1])
fields2 = split_field(fields)
print(fields)
print(fields2)

field = " __pivot_avg(age) AS `avg(age)`#238[0] AS 1#239"
# field = "`avg(age)`#238[0] AS 1#239"

# name_part = field.rsplit(" AS ", 1)[1]
# print(name_part)
# function_part = field.rsplit(" AS ", 1)[0]
# print(function_part)

ID_PATTERN1 = r"(?<!lambda )[^\w`](\w[\w\-`]*\#\d*[L]?)"
ID_PATTERN2 = r"(?<!lambda )[^\w`]([\w\-`\(\)]*\#\d*[L]?)"

f = "__pivot_avg(age) AS avg(age)#238[0]"
src_fields = list(set(re.findall(ID_PATTERN1, f)))
print(src_fields)
src_fields2 = list(set(re.findall(ID_PATTERN2, f)))
src_fields2 = [s for s in src_fields2 if not any(sf in s for sf in src_fields)]
print(src_fields2)


# split_fields = {}
# split_fields[name_part] = src_fields if len(src_fields) > 0 else ["__none__" if function_part == "null" else "__literal__"]
# print(split_fields)

# ids = list(set(re.findall(ID_PATTERN, line)))
# print(ids)

# parameters = "[age#617, names#618, {'range': {'start': 1, ' end': 2}, 'length': 2} AS lit2#7]"


# fields = strip_outer_parentheses(parameters[1:-1])
# print(fields)

# fields2: dict[str, list[str]] = {}
# for field in fields:
#     if " AS " in field:
#         name_part = field.rsplit(" AS ", 1)[1]
#         function_part = field.rsplit(" AS ", 1)[0]

#         pattern = ID_PATTERN
#         src_fields = list(set(re.findall(pattern, function_part)))
#         fields2[name_part] = src_fields if len(src_fields) > 0 else ["__none__" if function_part == "null" else "__literal__"]
#     else:
#         fields2[field] = [field]

# for k, v in fields2.items():
#     print(f"{k}: {v}")


# line = "Generate explode(m#649),false, [key#650, value#651]"
# result = re.split(r', ?(?![^\[]*\])', line)
# fields = list(set(re.findall(ID_PATTERN, result[2])))
# fields2 = list(set(re.findall(ID_PATTERN, result[0])))
# print(result)
# print(fields)
# print(fields2)


# logger = logging.getLogger(__name__)

# a = {"x": 1}

# try:
#     b = a["c"]
# except KeyError as e:
#     logger.exception("%s: field is not parsed properly", "x", exc_info=e)

# print(a)
