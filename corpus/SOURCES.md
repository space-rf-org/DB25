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
| `pg_case` | `src/test/regress/sql/case.sql` | PostgreSQL (`postgres/postgres`) | PostgreSQL License (permissive) |
| `lateral` | `pg/lateral.sql` (**committed** under `corpus/pg/`) | DB25 (authored, not harvested) | this repo's license |

The `lateral` session is the one **DB25-authored** source: a hand-written
Postgres-canon session (`corpus/pg/lateral.sql`, vendored because it is ours)
exercising every accepted `LATERAL` join shape — comma / `CROSS` / `[INNER]` /
`LEFT JOIN LATERAL`, an earlier-sibling correlation, and an uncorrelated lateral
body — end to end. The honest rejections (`RIGHT`/`FULL`/`NATURAL JOIN LATERAL`)
live in the parser's negative tests, not here.

PostgreSQL is DB25's canonical dialect anchor, so its regression SQL is the
closest source to the canon and the natural one to grow (`select`, `join`,
`aggregates`, … next). `case.sql` is self-contained (it creates its own tables)
and standard — no dialect transforms were needed; only non-core statement kinds
are excluded (see the harvest policy below).

## Documented divergences

The corpus is honest about where DB25 currently differs from the canon; the
golden records the real behaviour rather than hiding it:

- **`pg_case` — unquoted identifiers are case-sensitive.** `case.sql` writes both
  `CASE_TBL` (in the CREATE) and `case_tbl` (in one query). SQL folds unquoted
  identifiers to a single case, so both should name one table; DB25 matches them
  case-sensitively, so the lowercase reference resolves as an unresolved table
  (one `diag` row in the golden). A candidate follow-up fix, surfaced by this
  corpus.

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
