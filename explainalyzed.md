# Explainalized: Tool ot extract lineage from execution plan of pyspark query

Derives from the steps in the execution plan the table and column lineage.

## Test 

- select
- filter
- derive
- union
- join
- rename
- aggregate
- type conversion
- from table / create df
- all datatypes



cache, persist adds a physical plan to the logical plan:
- wrapper to convert into write_read and add cache/persist to temp dataset name.

use path to wrap pyspark write, cache and collect to write query plans