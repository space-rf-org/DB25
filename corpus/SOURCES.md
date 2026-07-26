# Corpus sources & provenance

All statements in `corpus.tsv` are harvested from the sources below, then
dialect-corrected / filtered to DB25's canon by `generate.py`. The upstream
files are **not vendored** (they are large); `generate.py` documents the exact
extraction and classification, and this file records where they came from so
the corpus is reproducible.

| session | upstream file | project | license |
|---------|---------------|---------|---------|
| `select1` | `test/select1.test` | sqllogictest (`gregrahn/sqllogictest`, mirror of the original SQLite suite) | Public Domain (SQLite) / MIT (sqllogictest harness) |
| `in1` | `test/evidence/in1.test` | sqllogictest (`gregrahn/sqllogictest`) | Public Domain / MIT |

## Harvest policy (implemented in `generate.py`)

sqllogictest records are self-labeling. We use that labeling directly:

- `statement ok` / `query …` → expected **accept** (`source_tag = ok`).
- `statement error` → expected **reject** (`source_tag = error`); note the
  source's errors are often *runtime* (constraint/type) which a frontend rightly
  accepts, so `source_tag` is reference, not a hard oracle — the golden
  `db25_parse` column records DB25's actual behaviour.
- `onlyif <engine>` / `skipif <engine>` gate a record to specific engines. We
  keep only what applies to DB25's Postgres-leaning canon: `onlyif <non-pg>` and
  `skipif postgresql` are excluded (with a counted reason).

Dialect constructs outside the canon (SQLite blob literals `x'…'`,
`AUTOINCREMENT`, `WITHOUT ROWID`, `PRAGMA`, `GLOB`, …) are excluded with a named
reason. See `COVERAGE.md` for the exact counts.

## Extending

Add entries to `SOURCES` in `generate.py` (Postgres's `src/test/regress/sql/*`
is the natural next source — it is closest to the canon and ships authored error
expectations), re-run the regeneration steps in `README.md`, and review the
resulting `corpus.tsv` / `COVERAGE.md` diff.
