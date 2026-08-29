# Corpus coverage

Sources (see SOURCES.md for provenance + licensing):

- `sample_select1.test.slt` (slt) -> session `select1` (<= 150 in-scope)
- `sample_in1.test.slt` (slt) -> session `in1` (<= 400 in-scope)
- `pg/case.sql` (pg) -> session `pg_case` (<= 400 in-scope)
- `pg/lateral.sql` (pg) -> session `lateral` (<= 50 in-scope)

- records scanned: 438
- in-scope (kept): 333
- excluded: 105

## Exclusions by reason (no silent drops)

- `engine-only:sqlite`: 82
- `out-of-scope:create-function`: 5
- `out-of-scope:explain`: 3
- `out-of-scope:begin`: 3
- `out-of-scope:rollback`: 3
- `dialect:sqlite-blob-literal`: 2
- `engine-only:mysql`: 2
- `out-of-scope:create-domain`: 2
- `out-of-scope:create-operator`: 2
- `out-of-scope:create-type`: 1

## In-scope by session

- select1: 150
- in1: 130
- pg_case: 45
- lateral: 8

## In-scope by category

- query: 256
- dml: 58
- ddl: 19
