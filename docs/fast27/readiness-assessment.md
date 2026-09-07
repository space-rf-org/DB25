# Readiness assessment

What exists across the nine repos, measured rather than claimed, and the gap
between that and a submittable systems paper. Written to be useful whichever
venue you target — the asset inventory and the gap list don't change with the
conference; only the scope verdict does.

Measured 2026-09-07 on the checked-out working copies.

---

## 1. Asset inventory

Non-test source lines exclude vendored `external/` submodules.

| Repo | Source LOC | Test files | Commits | State |
|---|---:|---:|---:|---|
| `db25-sql-parser` | 32,359 | 58 | 84 | Mature — arena AST, LATERAL joins landing |
| `DB25-Semantic-Analyzer` | 17,102 | 5 | 76 | Mature — CTAS, 3VL typing |
| `db25-physical-plan` | 14,428 | 14 | 44 | Increment 5.1 — memo, cost, plan cache |
| `db25-logical-plan` | 14,286 | 3 | 80 | Mature — binder + rewrite rules |
| `DB25` (umbrella) | 9,366 | 11 | 56 | Harnesses, corpus, docs |
| `DB25-sql-tokenizer` | 4,784 | 7 | 32 | Mature — SIMD lexer |
| `DB25-sql-tokenizer-token-packing` | 2,600 | 2 | 4 | Experimental branch of the above |
| `pki-service` | 2,082 | 0 | 86 | Unrelated to any DB25 paper |
| **`db25-execution-ref`** | **0** | **0** | **0** | **Empty. No commits.** |

Plus: 47 staged golden fixtures (`DB25/corpus/staged/`), a 344-line SQL corpus
(`DB25/corpus/corpus.tsv`), and two written papers with PDFs
(`DB25-sql-tokenizer/papers/`, `DB25/docs/arena-allocator/`).

**~95k lines of tested C++23 across six real stages.** This is a serious system.
The problem addressed in [`venue-fit.md`](venue-fit.md) is where to send it, not
whether it's worth sending.

## 2. The strong cards

These are genuine differentiators and should be foregrounded in any submission,
to any venue.

**The falsifiability gate.** The suite must pass clean, every injected mutant
must be caught by ≥1 test, and any test that *no* mutant can make fail is
reported as a defect — a vacuous test. Most systems papers claim "comprehensive
tests"; almost none can show that their tests are capable of failing. This is a
quantified, mechanized answer to a question reviewers rarely see answered.

**Per-stage golden s-expressions.** All five (soon six) pipeline artifacts have a
canonical s-expr rendering, pinned per stage, so a regression localizes to the
first diverging stage. Plus `staged_roundtrip` (the encoding is lossless) and
`staged_inject` (the optimizer is testable in isolation on committed input).

**The pure-function planner contract.** `plan(logical_plan, catalog_stats,
capability_profile, runtime_profile?)` with nothing leaking in except through
typed inputs — which is what lets the planner be golden-tested before any engine
exists. This is the most novel architectural idea in the stack.

**The gap register.** `docs/gap-register.md` inventories every known frontend gap
under a stated safety invariant: a deferred gap must fail honestly or be a
documented divergence, and can never silently produce a wrong result. Each entry
names the pin that keeps it honest. Reviewers respond well to a paper that
volunteers its own limitations with this much structure.

**Reference-evaluator equivalence checking.** Rewrites are verified by
`eval(bound) == eval(optimized)` over concrete tables with three-valued logic —
result equivalence, not plan shape. Plus a seeded property fuzzer, metamorphic
tests, and a decorrelation oracle.

**CI discipline.** Every suite under AddressSanitizer + UBSan, GCC 14, `-fno-exceptions`.

## 3. The gaps

Ordered by how badly each one blocks a submission.

| # | Gap | Blocks | Severity |
|---|---|---|---|
| 1 | **No execution engine.** `db25-execution-ref` is empty. | Any paper needing wall-clock results | **Blocking** for FAST; survivable at CIDR |
| 2 | **No I/O in the cost model.** `calibration.lab.sexpr` prices CPU-per-row only; no page-read, device-latency, or seq/random terms. | Any storage claim | **Blocking** for FAST |
| 3 | **No storage layer.** No buffer pool, page layout, file format, write path, or durability mechanism anywhere. | Any storage claim | **Blocking** for FAST |
| 4 | **No end-to-end performance evaluation.** No TPC-H/TPC-DS/JOB run, no comparison against DuckDB/SQLite/Postgres on a shared workload. `docs/engine-comparison-findings.md` is a design comparison, not a measured one. | Every venue | **Blocking** |
| 5 | **No baseline comparison for the optimizer.** Plan quality is pinned against DB25's own goldens — self-consistent, but there is no evidence the plans are *good*, only that they're stable. | Every venue | **High** |
| 6 | **Benchmarks are microbenchmarks.** `bench_parser.cpp`, `bench/reference_query.sql`. Nothing at query-workload scale. | Every venue | **High** |
| 7 | **Numeric inconsistencies in the tokenizer paper.** See §4. | Reuse of that paper | **High** — fix before resubmitting anywhere |
| 8 | **No related-work section anywhere.** The design docs cite no literature. A conference paper needs 30–50 references situating DB25 against Cascades/Volcano, Orca, Calcite, DuckDB, Umbra, Photon, SIMD-parsing work (Mison, simdjson, Sparser). | Every venue | **High** — 2–3 days of work |
| 9 | **Everything is single-author, single-machine.** No reproducibility across hardware, no variance reporting, no confidence intervals. | Evaluation credibility | Medium |
| 10 | **The whole stack is de-anonymized.** Author name in design docs, papers, commit history; `space-rf-org` throughout; distinctive repo names trivially findable. | Any double-blind venue | **High** — mechanical but easy to botch |

## 4. Two numbers that will not survive review

Both are in `DB25-sql-tokenizer/papers/db25-tokenizer-paper.tex`. Fix them before
that paper goes anywhere.

**(a) The headline throughput numbers are mutually inconsistent.** The abstract
claims *both* "over 20 million tokens per second" and "17.7 MB/s average
throughput" (§V reports 17.5 MB/s). Those imply **~0.88 bytes per token**. Real
SQL averages roughly 5–8 bytes per token including whitespace and delimiters, so
17.7 MB/s corresponds to roughly **2–3.5M tokens/s** — the two headline figures
disagree by a factor of 6–10×.

A reviewer will do this division in the first two minutes and it will cost the
paper its credibility on everything else. Either the tokens/s figure is measured
on a synthetic dense-token input that isn't representative, or one of the two is
simply wrong. Resolve it, state the bytes-per-token of the benchmark corpus
explicitly, and lead with whichever number is defensible.

**(b) The 4.5× speedup baseline is self-defined.** The comparison is against "a
traditional scalar implementation" — apparently DB25's own scalar path, not
PostgreSQL's or MySQL's real lexer. The intro cites those systems at 2–5 MB/s
from secondary sources rather than from a run. Speedup over your own unoptimized
code is a weak baseline; a reviewer will ask for a measured comparison against at
least one production lexer on the same corpus and the same machine.

Related, smaller: the arena paper's design goal says "sub-5 nanosecond" while its
abstract claims "sub-10 nanosecond." Pick one.

## 5. What is genuinely ready today

If you strip out everything that needs new measurement, this is publishable
material right now, at the right venue:

- The architecture: one repo per stage, s-expr artifacts at every boundary,
  spec-driven Cascades, the pure-function planner contract with capability and
  runtime profiles as typed inputs.
- The methodology: falsifiability gating, staged goldens with localization,
  round-trip and injection testing, the gap register with its safety invariant.
- Two hardware-conscious component results (tokenizer, arena) — after §4 is fixed.

That is a **CIDR** paper, or a **DaMoN** paper, essentially today. It is not a
FAST paper in any amount of time under two weeks.
