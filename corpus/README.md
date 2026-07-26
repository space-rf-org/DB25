# DB25 frontend SQL corpus

A committed collection of real SQL statements, harvested from public test
suites and **dialect-corrected to DB25's canon** (standard SQL, Postgres-leaning
where the standard is silent), used to exercise the frontend (tokenize → parse →
analyze → plan) at scale. It is judged by DB25's own principles — not by the
source engine's results.

## Why (and why not results)

DB25 is a SQL *frontend*: it has no executor, so we cannot compare result rows.
What we *can* do is take the enormous breadth of real SQL these suites contain
and pin the **artifact each frontend stage produces**. The corpus is the input
material; DB25's principles are the judge.

## Tiers

`corpus_runner` (see `../integration/corpus_runner.cpp`) replays the corpus and
enforces two tiers:

- **Tier R — robustness.** Every statement runs parse → analyze without a crash
  or undefined behaviour. Under the ASan/UBSan CI job this is the safety net:
  hundreds of adversarial real statements fuzz the frontend on every push.
- **Tier E — expectation.** Each statement carries committed `db25_parse` /
  `db25_analyze` columns (the golden). The runner recomputes them and fails if
  any differs, so a change that alters how the frontend parses or analyzes any
  statement cannot merge until the golden is regenerated **and reviewed**.

Statements replay per **session** (one per source): DDL builds a live catalog
that later DML / queries in the same session resolve against; each session
starts from a fresh catalog.

## No silent drops

Every statement the harvester excludes (engine-specific, or a dialect construct
outside DB25's canon) is counted against a **named reason** in
[`COVERAGE.md`](COVERAGE.md). Coverage is auditable: nothing is dropped quietly.

## Layout

| file | role |
|------|------|
| `corpus.tsv` | the golden: `session · source_tag · category · db25_parse · db25_analyze · sql` |
| `COVERAGE.md` | how many statements were scanned / kept / excluded, and why |
| `SOURCES.md` | upstream provenance + licensing for every source |
| `generate.py` | re-harvests + re-classifies from the upstream sources (documents the transform / exclusion rules) |

## Regenerating

```
# 1. re-harvest + re-classify from the upstream sources (see SOURCES.md)
python3 corpus/generate.py
# 2. fill the db25 behaviour columns from the current frontend, then review the diff
./build/corpus_runner --update corpus/corpus.tsv
git diff corpus/corpus.tsv        # every changed row is a behaviour change to explain
```

A golden change is never rubber-stamped: each flipped row is a real
parse/analyze behaviour change (a fix, a regression, or new coverage) and must
be understood before it is committed.
