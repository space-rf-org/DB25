# DB25

Umbrella / meta repository for the **DB25** SQL engine — the home for cross-cutting
architecture, methodology, and research documents that span more than one stage of the
engine.

DB25 is a from-scratch SQL engine built one repo per stage:

| Stage | Repo |
|-------|------|
| Tokenizer (SIMD lexer) | `DB25-sql-tokenizer` |
| Parser (arena AST) | `db25-sql-parser` |
| Semantic analyzer | `DB25-Semantic-Analyzer` |
| Binder + logical optimizer | `db25-logical-plan` |
| Physical planner / executor / storage | *(planned)* |

This repository holds the documents — and the cross-stage **end-to-end test harness** —
that don't belong to any single stage.

## End-to-end test harness

`harness/` + `tests/` build a full-pipeline test harness that drives
tokenizer → parser → analyzer → binder → optimizer (via the `db25-logical-plan`
submodule under `external/`) and checks correctness pessimistically. It centers on two
ideas:

- A **reference evaluator** with three-valued (NULL) logic that executes a logical plan
  over concrete in-memory tables, so a rewrite is checked by *result equivalence*
  (`eval(bound) == eval(optimized)`), not just plan shape.
- A **falsifiability gate** (`db25_gate`): the suite must pass clean, every injected
  mutant must be caught by ≥1 test, and any test that no mutant can make fail is reported
  as *non-falsifiable (vacuous)*. This keeps tests honest — a test that cannot fail is a
  defect.

Build & run:

```
git submodule update --init --recursive
cmake -S . -B build && cmake --build build -j
./build/db25_harness      # all suites (corpus / property-fuzz / metamorphic / smoke)
./build/db25_gate         # falsifiability gate
```

Test suites: `tests/corpus/` (hand-picked pessimistic cases — NULL/3VL, empty relations,
subqueries, folding boundaries, degenerate joins), `tests/property/` (deterministic
seeded SQL fuzzer + invariants), `tests/metamorphic/` (result-preserving rewrites + a
decorrelation oracle), `tests/smoke/`.

See [`docs/harness-findings.md`](docs/harness-findings.md) for the first-run results —
the harness surfaced four genuine `db25-logical-plan` bugs on day one.

## Contents

- [`docs/formal-methods-proposal.md`](docs/formal-methods-proposal.md) — a proposal for
  where TLA+ and Alloy would fit and be beneficial across the engine's lifecycle.
- [`docs/engine-comparison-findings.md`](docs/engine-comparison-findings.md) — a
  consolidated comparison of DB25 against FoundationDB, DuckDB, Polars, and SQLite,
  with ranked, actionable inspiration for the next layers.
- [`docs/harness-findings.md`](docs/harness-findings.md) — first-run harness results and
  the bugs/vacuous-tests it surfaced.
- [`docs/sql-surface.md`](docs/sql-surface.md) — the SQL surface the **whole stack**
  accepts (stack-tested), reconciled against the parser's permissive grammar.
- [`docs/arena-allocator/`](docs/arena-allocator/) — the arena / cache-aligned
  allocation strategy paper, a stack-wide methodology (relocated from the parser).
