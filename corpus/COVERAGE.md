# Corpus coverage

Sources (sqllogictest; SQLite = public domain, sqllogictest = MIT):

- `sample_select1.test.slt` -> session `select1` (<= 150 in-scope)
- `sample_in1.test.slt` -> session `in1` (<= 400 in-scope)

- records scanned: 366
- in-scope (kept): 280
- excluded: 86

## Exclusions by reason (no silent drops)

- `engine-only:sqlite`: 82
- `dialect:sqlite-blob-literal`: 2
- `engine-only:mysql`: 2

## In-scope by session / category

- select1: 150
- in1: 130

- query: 222
- dml: 45
- ddl: 13
