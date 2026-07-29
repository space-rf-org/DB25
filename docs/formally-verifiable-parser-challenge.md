# DB25 Formally-Verifiable SQL Parser — Community Challenge (Bootstrap)

**Status:** proposal / RFC / bootstrap
**Date:** 2026-07-29
**Scope:** a *second*, from-scratch SQL parser for the DB25 front-end, built to be
**formally verifiable** and **provably total**, developed as an open community
challenge — alongside (not replacing) the existing hand-written parser, which we
keep and structurally fix in parallel.
**Companion doc:** [`formal-methods-proposal.md`](formal-methods-proposal.md)
(where TLA+/Alloy fit across the engine). This doc is the parser-specific,
grammar-and-totality-focused counterpart.

> This is a **bootstrap**. It records everything we know today and fixes the
> non-negotiables (the totality contract and the shared conformance oracle).
> Everything not yet decided is called out explicitly as a **TODO** rather than
> guessed. Nothing here is final until an implementation track ratifies it.

---

## 0. TL;DR

- **Two tracks, one oracle.** Track A: keep the existing `db25-sql-parser` and fix
  it structurally (see §5, issues [#71]/[#72]). Track B: a community challenge to
  build the *best formally-verifiable* SQL parser from scratch. Both are validated
  against **the same conformance corpus** — a rigorous *one SQL → one canonical AST
  or one diagnostic* golden set (§4). The corpus is the real cross-cutting
  deliverable; it is what makes "best" measurable and what de-risks the fix.
- **The bar is totality, not just correctness.** The defect that motivated this
  (see §1) is that today's parser has a silent third state: *success with a partial
  AST*. The challenge target eliminates that **by construction**: parsing must be a
  **total function** `sql ↦ { unique AST | diagnostic }` — always terminating,
  never partial, never crashing.
- **Architecture is open; the contract is not.** Entrants may use PEG/packrat, a
  verified LR generator (Menhir+Coq), total parser combinators in a dependently-typed
  language (Agda/Idris/Lean), or anything else — *provided* they can demonstrate the
  totality contract and pass the conformance oracle. §3 lists what "formally
  verifiable" is allowed to mean, ranked by strength of evidence.
- **Bootstrap now, build later.** This commit lands the proposal + the corpus schema
  + the scoring rubric. Implementation, tooling, and the full corpus are TODOs.

---

## 1. Why (the motivating defect)

A thorough analysis of the existing hand-written recursive-descent + Pratt parser
(`space-rf-org/db25-sql-parser`) found that **malformed SQL parses "successfully"
with a silently truncated AST**. Root cause is structural, not a scatter of local
bugs:

- Parse helpers signal failure only by returning `nullptr`, and `nullptr` already
  means "optional element legitimately absent." The expression layer therefore
  chooses to return a *partial* node (`return left;`) rather than a null it can't
  disambiguate — laundering malformed input into a clean success.
- `parse()` defines success as "top parser returned non-null," **not** "…and all
  input consumed **and** no error recorded." Leftover tokens are reconciled into a
  non-fatal counter.

Result: a dropped `WHERE`/`JOIN ON` predicate, a wrong `DROP TABLE` target, or a
structurally invalid tree (a required child missing) can all be reported as a
successful parse. Full write-up and reproductions: issues [#71] (silent
truncation) and [#72] (OOM null-deref). This is exactly the class of defect a
*provably total* parser makes unrepresentable.

---

## 2. The challenge, in one paragraph

> **Build the SQL parser with the strongest machine-checkable guarantee that every
> input string maps to exactly one outcome — a unique, canonical AST or a precise
> diagnostic — with no silent partials, no crashes, and no non-termination, while
> matching the DB25 conformance corpus.** Strength of the guarantee, breadth of the
> accepted SQL surface, and quality of diagnostics are the scored axes (§6).

---

## 3. What "formally verifiable / provably total" is allowed to mean

Ranked by strength of evidence (a submission declares its tier and must back it up):

| Tier | Guarantee | Typical realization | Evidence required |
|---|---|---|---|
| **T3 — Proven** | Machine-checked proof that the parser recognizes *exactly* the grammar's language and is total | Menhir `--coq` validated LR(1) (CompCert-style); total parser combinators in Agda/Idris/Lean with termination by construction | The proof artifact + checker in CI |
| **T2 — Total by construction** | Termination + single-outcome guaranteed by the formalism, grammar unambiguous by construction | PEG/packrat (ordered choice ⇒ ≤1 parse; memoization ⇒ linear, terminating), scannerless | Grammar file as source of truth + a totality property harness (§4.3) green |
| **T1 — Contract-enforced** | Totality holds as an enforced contract, checked empirically at scale | Any architecture + the §4.3 totality fuzzer + full corpus, strict-by-default | Property harness + corpus green, differential vs a reference |

Notes / knowns:
- **Totality (precise definition).** `parse : String → Result` is *total* iff for
  every input it terminates and returns exactly one of `{ Ast, Diagnostic }` — never
  a partial AST, never undefined behavior, never a hang. "Provable" = established
  mechanically (T3), by the formalism (T2), or by an enforced+fuzzed contract (T1).
- **You do not escape the hard parts.** Left-recursive expressions (`a - b - c`) and
  operator precedence still need precedence-climbing/Pratt handling even under PEG;
  PEG's ordered choice removes *ambiguity* but can silently pick a *worse* single
  parse (prefix-capture, e.g. `IN` shadowing `IN (...)`) — so *unambiguous ≠
  correct*, and the corpus (§4) is what catches "correct-but-wrong-tree."
- **Diagnostics are scored, not free.** Generated/backtracking parsers historically
  regress error quality; farthest-failure tracking or equivalent is expected.
- **AST construction stays hand-written** in every approach; the formalism/generator
  covers *recognition*, not tree-shaping — so the canonical-AST contract (§4.1)
  binds all tracks equally.

**TODO (T3/T2 specifics):**
- [ ] Pick the reference proof toolchain(s) we will accept and CI-check (Coq/Menhir?
      Lean 4? Agda?). Define the minimum proof obligation (soundness + completeness
      vs the grammar; totality).
- [ ] Decide whether the *grammar* (not the parser) is the normative spec, and in
      which notation (see §4.2 — we already ship EBNF).
- [ ] Define how a proof artifact is re-checked in CI without a fragile toolchain.

---

## 4. The shared conformance oracle (the backbone for BOTH tracks)

The user requirement: *every single line of SQL must have one strict expected AST,
and we validate rigorously against it.* This section defines that oracle. It is the
single most important deliverable here — it is what makes the challenge objective and
what protects the Track-A fix from regressions.

### 4.1 The contract: canonical AST or diagnostic

Define parsing as a **total function** with a **canonical, deterministic
serialization** of the AST:

```
outcome(sql) := ACCEPT(canonical_ast_text)   -- exactly one, stable, byte-for-byte
             |  REJECT(diagnostic)            -- precise, positioned
```

- **Canonical AST text.** One stable textual form per tree (node type + payload +
  ordered children), independent of parser internals, so two conforming parsers
  produce *identical* bytes for the same SQL. We already have serializers to build
  on: `db25-sql-parser/tools/ast_dumper.cpp`, `ast_to_dot.cpp`,
  `dump_complex_ast.cpp`. **TODO:** freeze ONE canonical grammar-level format
  (proposed: S-expression, e.g. `(Select (Project (Col a)) (From (Table t)) (Where
  (= (Col x) (Int 1))))`) that is *parser-agnostic* — not tied to the 128-byte node
  layout.
- **No third state.** `ACCEPT` requires the whole input consumed and no recorded
  error; anything else is `REJECT`. This is precisely what Track A must adopt (§5)
  and what Track B gets for free at T2/T3.

### 4.2 Grammar as source of truth

We already ship grammar artifacts in the parser repo:
`grammar/DB25_SQL_GRAMMAR.ebnf`, `ebnf_supported.sql` (334 lines, the surface the
stack accepts today) and `ebnf_complete.sql` (565 lines, the aspirational surface),
plus the umbrella [`docs/sql-surface.md`](sql-surface.md) (stack-tested surface).

**TODO:**
- [ ] Elevate ONE grammar to normative status for the challenge and reconcile it
      against `sql-surface.md` and `ebnf_supported.sql`.
- [ ] Machine-readable form (EBNF/PEG) that can *generate* positive corpus cases and
      *drive* the totality fuzzer (§4.3).

### 4.3 Corpus shape: positive, negative, and totality

Three parts — all currently **partial**; making them rigorous is the work:

1. **Positive corpus** — `sql ⇒ expected canonical AST`. Every accepted input pinned
   to its one tree. *Have today:* umbrella `corpus/corpus.tsv` — **325** curated
   statements (sessions `select1`/`in1`/`pg_case`) tagged with `db25_parse` /
   `db25_analyze` expectations and provenance (`corpus/SOURCES.md`,
   `corpus/COVERAGE.md`, "no silent drops" exclusion ledger). **Gap:** it records
   *accept/reject + analyze status*, **not** the full expected AST. **TODO:** add a
   canonical-AST column (§4.1) for every row.
2. **Negative corpus** — `malformed sql ⇒ expected diagnostic (class + position)`.
   This is the *direct* test of the motivating defect: each of the §1 pathologies
   (`WHERE a =`, `a JOIN b ON`, `SELECT 1 UNION`, `DROP TABLE s.t`, …) becomes a
   required `REJECT` case. *Have today:* scattered negative tests in
   `db25-sql-parser/tests/` (e.g. `test_parser_negative.cpp`,
   `tests/security/test_depth_guard.cpp`) — not centralized, not exhaustive.
   **TODO:** build the canonical negative corpus from the issue-#71 table + a
   mutation generator.
3. **Totality property harness** — the machine-checkable "no third state" invariant:
   for a large, grammar-derived + mutated + fuzzed input set, assert *every* input
   yields `ACCEPT` (whole input consumed, no error) or `REJECT` — **never** a
   partial AST, crash, hang, or OOM. *Have today:* the umbrella `db25_harness` +
   `db25_gate` (falsifiability gate) and parser property tests are the seed; the
   totality invariant itself is **not** yet asserted. **TODO:** implement it.

### 4.4 Equivalence, not just shape

Two parsers can agree on tree *shape* and still be wrong. Bind the corpus to
*meaning* by running accepted trees through the umbrella **reference evaluator**
(`harness/harness.cpp`, three-valued logic) and comparing *results* on sample data —
reusing the existing `eval(bound) == eval(optimized)` machinery. **TODO:** wire the
challenge oracle into the umbrella harness so `ACCEPT` cases are also result-checked.

### 4.5 Provenance & licensing (keep it honest)

The existing corpus already tracks source + license per case and logs every
exclusion with a reason (no silent drops) — see `corpus/SOURCES.md` /
`corpus/COVERAGE.md`. The challenge corpus inherits this discipline. **TODO:**
confirm licensing of any new third-party SQL (SQLite SLT, PostgreSQL regress, DuckDB,
etc.) before import.

---

## 5. Track A — keep and structurally fix the existing parser

We are **not** waiting on the challenge to fix production. In parallel:

1. **Unify the failure contract.** Give helpers a real two-channel result
   (distinguish `Absent` from `Error`, e.g. `expected<ASTNode*, Diag>` or a
   `{node,status}` pair) so `nullptr` stops meaning two things; route the ~17
   soft-bail sites through `error()`.
2. **Redefine success.** `parse()` returns the AST iff *non-null root ∧ input at EOF
   ∧ no recorded error*; else a diagnostic. Flip strict-by-default.
3. **Centralize construction.** Route all **149** raw `arena_.allocate<ASTNode>`
   sites through the null-checking `make_node()` (closes [#72], restores cohesion).
4. **Re-baseline tests** against the strict contract — the "~8 regressed suites" are
   exactly the lenient-wrong cases the corpus (§4) now nails down.

Rationale for fix-over-rewrite (measured): the grammar breadth, precedence, and the
AST/arena/tokenizer substrate are already correct on well-formed SQL; the defect is
concentrated (~17 fail-open sites vs 114 already fail-closed). Full reasoning lives in
issues [#71]/[#72]. **The §4 corpus is what makes this fix safe.**

---

## 6. Scoring the challenge (rubric, draft)

A submission is ranked on four axes; **passing the conformance oracle (§4) is a gate,
not a score** — you cannot place without 100% on the frozen corpus + zero totality
violations.

| Axis | Weight (draft) | What it measures |
|---|---|---|
| **Guarantee tier** | high | T3 > T2 > T1 (§3), with evidence re-checked in CI |
| **SQL surface** | high | Fraction of `ebnf_complete.sql` / normative grammar accepted, corpus-verified |
| **Diagnostics** | medium | Negative-corpus precision: right error class + position |
| **Performance** | medium | Throughput + memory vs the SIMD/arena baseline (secondary to correctness) |

**TODO:**
- [ ] Finalize weights and whether tiers are a multiplier vs an additive axis.
- [ ] Define the CI submission harness (container, pinned toolchains, proof re-check).
- [ ] Governance: how a submission is verified, by whom, and how ties break.
- [ ] License / CLA for entries; where winning entrants' code lives.

---

## 7. Proposed layout (bootstrap)

Bootstrapped now (this commit):

```
DB25/docs/formally-verifiable-parser-challenge.md   <- this file
DB25/docs/formal-methods-proposal.md                <- existing companion
DB25/conformance/                 <- the shared oracle (bootstrapped)
  README.md          canonical S-expr format spec + how to run          [DONE]
  tools/ast_to_sexpr.cpp   reference canonical serializer               [DONE, builds]
  positive/goldens.sexpr   5 verified sql -> canonical AST goldens      [seed]
  positive/feature-gaps.tsv  accept-but-wrong rows of #71               [seed]
  negative/pathologies.tsv   12 malformed sql -> REJECT (lifted #71)    [seed]
# TODO (later):
  grammar/           normative machine-readable grammar (EBNF/PEG)
  totality/          property/fuzz harness asserting "no third state"
  (CMake wiring, run_corpus driver, make goldens, result-equivalence)
DB25/challenge/                   <- challenge rules, submission template, CI
```

**TODO:** decide whether the conformance oracle lives in the umbrella repo (shared,
cross-cutting — matches this repo's stated purpose) or in `db25-sql-parser` (closer
to Track A). Leaning umbrella, since Track B is a *separate* parser that must not
depend on Track A's internals.

---

## 8. Open questions (decide before leaving bootstrap)

- [ ] **Canonical AST format** (§4.1): S-expression vs JSON vs the existing dumper
      format — must be parser-agnostic and diffable. *Blocking for both tracks.*
- [ ] **Normative grammar** (§4.2): which artifact, which notation.
- [ ] **Accepted proof toolchains** (§3 T3): Coq/Menhir, Lean 4, Agda — and the CI
      re-check story.
- [ ] **Oracle home** (§7): umbrella vs parser repo.
- [ ] **Dialect scope**: how much beyond ANSI (the DB25 extensions — hex/binary/`.5`,
      `ARRAY[]`, `COLLATE`, `CAST` modifiers — per `sql-surface.md`) is in-scope for
      the frozen corpus.
- [ ] **Reject granularity**: is matching the *diagnostic class + position* required,
      or only ACCEPT/REJECT? (Affects how strict the negative corpus is.)

---

## 9. Immediate next steps (post-bootstrap)

1. Freeze the **canonical AST format** (§4.1) and write its spec — unblocks everything.
2. Add a **canonical-AST column** to `corpus/corpus.tsv` for all 325 rows (positive
   corpus v1).
3. Stand up the **negative corpus** from the issue-#71 pathology table.
4. Implement the **totality property harness** (§4.3) against Track A first — it
   immediately measures the fix.
5. Publish the **challenge rules + scoring** (§6) and a minimal submission template.

[#71]: https://github.com/space-rf-org/db25-sql-parser/issues/71
[#72]: https://github.com/space-rf-org/db25-sql-parser/issues/72
