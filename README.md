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

This repository holds the documents — the runnable **examples**, and the cross-stage
**end-to-end test harnesses** — that don't belong to any single stage.

## Getting started

**Prerequisites:** a C++23 compiler (g++-13+ / clang-16+), CMake ≥ 3.20, and Ninja or
Make. The whole stack is `-fno-exceptions`.

```sh
# 1. Clone with every stage (the stages are git submodules under external/).
git clone --recursive https://github.com/space-rf-org/db25
cd db25
#    (already cloned without --recursive? run: git submodule update --init --recursive)

# 2. Configure + build everything (stages, harnesses, examples).
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j

# 3. Run the whole test suite (see "Testing" below for what each test is).
ctest --test-dir build --output-on-failure

# 4. Run the guided examples - each prints every stage's output, s-expressions included.
./build/examples/db25_tour
./build/examples/db25_dialect_features
```

New to the codebase? Read **[`docs/tutorial.md`](docs/tutorial.md)** (a narrated walk
through the pipeline) with **[`examples/`](examples/)** (the runnable programs behind it)
open beside it. To *see* what a statement becomes at every stage, render the AST as a
tree diagram plus each stage's artifact as a self-contained HTML page:

```sh
./build/corpus_report --html corpus/showcase.sql > report.html   # then open in a browser
```

## Seeing the stages: s-expressions and the AST

Every pipeline artifact — token stream, parser AST, resolved (typed) AST, logical plan,
optimized plan — has a canonical **s-expression** rendering. `db25_tour` prints the
optimized-plan s-expr for each query, and the committed **staged fixtures**
(`corpus/staged/*.fixture`) hold all five layers for a set of representative statements,
so `cat corpus/staged/01_join_filter_project.fixture` is the fastest way to read what
each stage emits. For a visual AST, use `corpus_report --html` (above), which draws the
node tree with file-explorer guide lines alongside the plan artifacts.

## End-to-end test harnesses

Two complementary harnesses drive tokenizer → parser → analyzer → binder → optimizer
(via the `db25-logical-plan` submodule under `external/`) and check correctness
pessimistically. Everything below is wired into `ctest`.

**1. Reference-evaluator harness** (`harness/` + `tests/`) — checks *result equivalence*:

- A **reference evaluator** with three-valued (NULL) logic executes a logical plan over
  concrete in-memory tables, so a rewrite is verified by `eval(bound) == eval(optimized)`,
  not just plan shape (`db25_harness`).
- A **falsifiability gate** (`db25_gate`): the suite must pass clean, every injected
  mutant must be caught by ≥ 1 test, and any test that no mutant can make fail is reported
  as *non-falsifiable (vacuous)* — a test that cannot fail is a defect.

**2. Staged-artifact s-expression harness** (`integration/`, `corpus/staged/`) — pins the
canonical s-expr of *each stage* against a committed golden, localizing a regression to
the first stage that diverges. Run by `ctest`, it has four modes:

- `staged_runner` — produced artifact == golden, per stage;
- `staged_gate` — falsifiability: every plan golden must be caught by ≥ 1 mutation;
- `staged_roundtrip` — `write(read(golden)) == golden` (the s-expr is a lossless plan
  encoding);
- `staged_inject` — `optimize(read(logical)) == optimized golden` (tests the optimizer
  in isolation on a committed input).

## Testing

`ctest --test-dir build` runs it all: both harnesses above, the corpus replay
(`corpus_runner`), the per-stage binder / optimizer suites, and the `corpus_report`
renderers. The reference-evaluator suites live under `tests/corpus/` (hand-picked
pessimistic cases — NULL/3VL, empty relations, subqueries, folding boundaries, degenerate
joins), `tests/property/` (deterministic seeded SQL fuzzer + invariants),
`tests/metamorphic/` (result-preserving rewrites + a decorrelation oracle), and
`tests/smoke/`. CI additionally runs every suite under AddressSanitizer + UBSan.

## Contents

- [`docs/tutorial.md`](docs/tutorial.md) — a narrated, end-to-end walk through the
  pipeline; every plan and result in it comes from [`examples/`](examples/).
- [`docs/gap-register.md`](docs/gap-register.md) — the single inventory of every known
  frontend gap: each is either closed or a deferred gap that provably cannot produce a
  wrong result. The phase-exit checklist.
- [`docs/sql-surface.md`](docs/sql-surface.md) — the SQL surface the **whole stack**
  accepts (stack-tested), reconciled against the parser's permissive grammar.
- [`docs/coverage-catalog.html`](docs/coverage-catalog.html) — a rendered feature/coverage
  catalog across the stages.
- [`docs/harness-findings.md`](docs/harness-findings.md) — first-run harness results and
  the bugs / vacuous-tests it surfaced.
- [`docs/engine-comparison-findings.md`](docs/engine-comparison-findings.md) — a
  consolidated comparison of DB25 against FoundationDB, DuckDB, Polars, and SQLite,
  with ranked, actionable inspiration for the next layers.
- [`docs/formal-methods-proposal.md`](docs/formal-methods-proposal.md) — a proposal for
  where TLA+ and Alloy would fit across the engine's lifecycle.
- [`docs/arena-allocator/`](docs/arena-allocator/) — the arena / cache-aligned
  allocation strategy paper, a stack-wide methodology.
