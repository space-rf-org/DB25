# DB25 Frontend Gap Register

The single inventory of every **known gap** in the DB25 SQL frontend (tokenizer →
parser → analyzer → binder → optimizer). It exists so that finishing the frontend
phase is a checklist, not a judgement call: at phase exit, every gap here is either
**closed** or a **deferred known-gap that cannot cause a wrong result**.

## The safety invariant

> A deferred gap must **fail honestly** — a parse error, a bind error, or an
> analyzer diagnostic — or be an **intentional, documented divergence**. It must
> never silently produce a wrong result or a mis-typed / mis-shaped plan.

Deferral is only safe under this invariant. A gap that makes the pipeline *silently
wrong* is not deferrable — it is a **close** item and must be fixed before phase
exit. Every deferred entry below names the honest-rejection behavior that makes it
safe, and points at the fixture / test that pins that behavior so it cannot
regress into silence.

## Classification

- **DEFERRED** — safe under the invariant. Either an unimplemented *feature*
  (rejected honestly) or an intentional divergence. Fine to carry into the next
  phase.
- **CLOSE** — a correctness defect: a *legal* statement is wrongly rejected or
  mis-typed. It "causes issues" and must be closed before phase exit.

## Summary

| ID | Area | Gap | Class | Honest behavior / pin |
|----|------|-----|-------|-----------------------|
| G1 | binder | ROLLUP / CUBE / GROUPING SETS lowering | DEFERRED | `bind-error "GroupingElement not lowerable"` · `04,11,12` |
| G2 | binder | Quantified comparison `ALL`/`ANY`/`SOME` lowering | DEFERRED | `bind-error "quantified comparison … not yet supported"` · `26` |
| G3 | binder | CTAS / DDL statement lowering | DEFERRED | `bind-error "statement kind not yet lowered"` · `31` |
| G4 | binder | Qualified star (`u.*`) over a join | DEFERRED | not-yet-lowered arity guard (honest reject) |
| ~~G5~~ | harness | Phase-B s-expr **reader** coverage | ✅ CLOSED | reader covers every corpus construct; 0 `-- phaseb` deferrals — roundtrip 56/0, inject 28/0 |
| ~~G6~~ | harness | Full-fidelity plan **injection** (node-id serialization) | ✅ CLOSED | provenance ids (`:tid`/`:cid`) serialized on schema + colref/outerref; self-join fixture `32_self_join` injects lossless |
| ~~G7~~ | analyzer | `INTERVAL 'x'` literal typed `Unknown` | ✅ CLOSED | now `Interval` — analyzer #98 · `29_interval` |
| ~~G8~~ | parser | `EXTRACT(part FROM <typed literal>)` / `DATE '…'` value dropped | ✅ CLOSED | parser #83 |
| ~~G9~~ | analyzer | `CURRENT_DATE` unresolved (treated as a column) | ✅ CLOSED | niladic function — parser #83 · `30_extract` |
| G10 | pipeline | `1/0` preserved, not folded/rejected | DEFERRED (intentional) | soft `DivisionByZero` warning; documented PG divergence |

Fixtures are under `corpus/staged/`.

---

## Deferred known-gaps

### G1 — ROLLUP / CUBE / GROUPING SETS lowering
- **What:** the binder does not expand a `GroupingElement` (the parser's node for
  `ROLLUP()`/`CUBE()`/`GROUPING SETS()`) into its grouping sets.
- **Current behavior:** `bind-error "expression form 'GroupingElement' is not yet
  lowerable"`. The **analyzer is complete** here — grouping legality, and the
  super-aggregate nullability of keys and of expressions reading a key (but not
  window functions over a key), are all correct and unit-tested.
- **Safe because:** the statement stops at bind with a clear message; nothing
  downstream sees a mis-shaped plan. Pinned by `04_group_by_rollup`,
  `11_group_by_cube`, `12_grouping_sets`.
- **Fix (feature):** grouping-set expansion in the binder (one grouped input per
  set, unioned), then lower the `GROUPING()` indicator.

### G2 — Quantified comparison lowering (`x <cmp> ALL|ANY|SOME (subquery)`)
- **Current behavior:** analyzer types it `Boolean` and validates single-column
  arity; the binder rejects with `bind-error "quantified comparison (ALL / ANY /
  SOME) is not yet supported"`.
- **Safe because:** honest, specific bind error (never the old generic
  "unrecognized binary operator"). Pinned by `26_quantified_all`.
- **Fix (feature):** lower to a semi/anti-join or a scalar quantified expression.

### G3 — CTAS / DDL statement lowering
- **Current behavior:** DDL (`CREATE TABLE …`, incl. `NOT NULL` / `REFERENCES` /
  `CHECK`) parses and analyzes; the binder returns `bind-error "statement kind not
  yet lowered (TODO)"`. The corpus runner applies DDL to a live catalog via
  `execute_ddl`, so later statements resolve against it.
- **Safe because:** honest bind error; DDL is still fully parsed/analyzed and
  builds the catalog. Pinned by `31_ddl_constraints`.
- **Fix (feature):** a DDL/CTAS lowering path once storage exists.

### G4 — Qualified star over a join
- **What:** `SELECT u.* FROM u JOIN v …` — a qualified star whose relation is one
  side of a join.
- **Current behavior:** rejected via the documented not-yet-lowered arity guard
  (surfaced clean in the contract-seam audit).
- **Safe because:** honest rejection, not a silently wrong projection.
- **Fix (feature):** resolve the qualified star to its relation's columns during
  lowering.

### ~~G5~~ — Phase-B s-expr reader coverage — ✅ CLOSED
- **What:** `plan_from_sexpr` (the reader that round-trip and per-module injection
  build on) did not cover plans containing **window, set-op, limit, sort,
  recursive-CTE, LIKE, BETWEEN, IS NULL, boolean-test, CASE, IN-list, cast,
  quantified-subquery, interval, or EXTRACT** constructs.
- **Resolution:** the reader now handles every one of those node payloads
  (`:keys` for Sort, `:windows`, `:op` set-op, `:rows` Values) and expression
  kinds (`winfunc` + OVER spec, `cast`, `like`, `between`, `isnull`, `booltest`,
  `case`, `inlist`, `subquery` incl. quantified `:op`/`:quant`, `param`), mirroring
  the writer's grammar, plus the aggregate `:filter`. Operand collection was made
  valueless-flag-aware (`:distinct`/`:ci`/`:negated`/`:correlated`) so a bare flag
  no longer swallows a following operand. All `-- phaseb` markers are removed.
- **Verified:** `staged_runner --roundtrip` — 56 goldens, 0 not-lossless, **0
  deferred**; `--inject` — 28 goldens, 0 mismatches, **0 deferred**; `--gate` — 56
  goldens, 0 vacuous / 0 stale. The whole staged corpus is now round-tripped and
  injected strictly.

### ~~G6~~ — Full-fidelity plan injection (node-id serialization) — ✅ CLOSED
- **What:** lossless injection of plans (self-joins included) needs the binder's
  provenance identities serialized, not just positional column references.
  Originally tracked as "Phase B task #2".
- **Resolution:** the plan s-expr writer now renders the base-column provenance
  the binder resolves — `(table_id, column_id)` — as `:tid` / `:cid` on every
  schema column and on `colref` / `outerref` expressions (mirroring the
  resolved-AST layer), and the reader restores them. A read-back plan is now
  identical to the bound plan, not one with the ids dropped. Added
  `32_self_join` (`emp a JOIN emp b …`): the two instances are disambiguated by
  `input_index` + `:alias`, and it now round-trips lossless and injects
  (`optimize(read(logical)) == optimized golden`).
- **Verified:** `staged_runner --roundtrip` (0 not-lossless), `--inject` (0
  mismatches, self-join included), `--gate` (0 vacuous / 0 stale).

### G10 — `1/0` preserved (intentional divergence)
- **What:** a constant `1/0` is **not** folded or rejected at plan time.
- **Current behavior:** analyzer emits a soft `DivisionByZero` **warning** and the
  value flows through; the optimizer preserves it. This is a deliberate divergence
  from PostgreSQL (which folds and rejects), relied on by the corpus (`pg_case`).
- **Safe because:** intentional and documented; the warning is emitted, and dead
  arms suppress it (`CASE WHEN 1=0 THEN 1/0 …`).
- **Fix:** none intended — this is a design decision, recorded so it is not
  mistaken for a bug.

---

## Close-now queue

**Empty — all cleared.** The temporal set (G7, G8, G9) closed via parser #83 and
analyzer #98:

- **G7** — `INTERVAL` / `DATE` / `TIME` / `TIMESTAMP` literals now type as their
  concrete temporal type (were `Unknown`); pinned in `29_interval`.
- **G8** — `EXTRACT` accepts a full-expression operand, and `DATE` / `TIME` /
  `TIMESTAMP '…'` typed literals now parse (previously `DATE '…'` parsed but
  **silently dropped the value string** — a latent data-loss bug this surfaced).
- **G9** — `CURRENT_DATE` / `CURRENT_TIME` / `CURRENT_TIMESTAMP` now parse as
  niladic functions and resolve to their temporal type; `30_extract` binds to a
  real plan (was a `bind-error`).

New silently-wrong gaps found by `frontend-audit` land here first, classified,
before any fix.

---

*Maintenance: when a gap closes, move its row out of the table and delete its
section (or, for a feature, keep it under Deferred with an updated status). New
gaps found by `frontend-audit` land here first, classified, before any fix.*
