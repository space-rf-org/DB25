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
| ~~G1~~ | binder | ROLLUP / CUBE / GROUPING SETS lowering | ✅ CLOSED | first-class `Aggregate.grouping_sets`; `04,11,12` now bind + round-trip + inject |
| ~~G2~~ | binder | Quantified comparison `ALL`/`ANY`/`SOME` lowering | ✅ CLOSED | lowers to an owned `ExprKind::Subquery :kind quantified` (op + ALL/ANY sense) · `26` |
| ~~G3~~ | binder+catalog | DDL statement lowering | ✅ CLOSED (CTAS) / rescoped | CTAS → `CreateTableAs` (`36`) + `execute_ddl` registers the table; plain DDL is a catalog op, honestly not a query plan |
| ~~G4~~ | binder | Qualified star (`u.*`) over a join | ✅ CLOSED | expanded to the relation's columns in `lower_projection` · `35` |
| ~~G5~~ | harness | Phase-B s-expr **reader** coverage | ✅ CLOSED | reader covers every corpus construct; 0 `-- phaseb` deferrals — roundtrip 56/0, inject 28/0 |
| ~~G6~~ | harness | Full-fidelity plan **injection** (node-id serialization) | ✅ CLOSED | provenance ids (`:tid`/`:cid`) serialized on schema + colref/outerref; self-join fixture `32_self_join` injects lossless |
| ~~G7~~ | analyzer | `INTERVAL 'x'` literal typed `Unknown` | ✅ CLOSED | now `Interval` — analyzer #98 · `29_interval` |
| ~~G8~~ | parser | `EXTRACT(part FROM <typed literal>)` / `DATE '…'` value dropped | ✅ CLOSED | parser #83 |
| ~~G9~~ | analyzer | `CURRENT_DATE` unresolved (treated as a column) | ✅ CLOSED | niladic function — parser #83 · `30_extract` |
| G10 | pipeline | `1/0` preserved, not folded/rejected | DEFERRED (intentional) | soft `DivisionByZero` warning; documented PG divergence |
| ~~G11~~ | parser+binder | `LATERAL` joins unsupported (comma / `JOIN LATERAL` failed to parse) | ✅ CLOSED | `LateralJoin` node → correlated bind (`OuterRef`) + `JoinType::Lateral`/`LeftLateral`; parser #124/#125, analyzer #157/#158, LP #176/#177 · `33_lateral`, `34_left_join_lateral` |

Fixtures are under `corpus/staged/`.

Two defects were found by ADDING FIXTURES rather than by an audit, when the
physical planner learned to lower DML (increment 3.9b) and the corpus gained
`39`-`43`. Recorded here because both are the same shape - a stage that renders
less than it computes, so a golden cannot tell two different statements apart -
and that shape is worth recognising the next time.

- **The staged LOGICAL writer rendered DML as a bare `(Update :out ())`** - no
  target relation, no SET list, no column list, no ON CONFLICT, and a `Values`
  node as a row COUNT rather than its rows. Two different UPDATEs produced the
  same golden, so a fixture pinned on it could not fail. The writer and the
  reader now carry all of it, and the round-trip and injection gates cover it.
- **The binder left every DML node's `output` EMPTY** while a `Returning` above
  it projected `col #0` of those columns - a projection over a zero-column
  input, which no executor could run. It was invisible for as long as nothing
  looked at the schema BETWEEN the two nodes; the physical planner renders both.
  Fixed in db25-logical-plan#180.

---

## Deferred known-gaps

### ~~G1~~ — ROLLUP / CUBE / GROUPING SETS lowering — ✅ CLOSED
- **What:** the binder rejected a `GroupingElement` (the parser's node for
  `ROLLUP()`/`CUBE()`/`GROUPING SETS()`) group key.
- **Resolution:** the binder now flattens the `GroupingElement` into the flat,
  de-duplicated leaf grouping columns plus a first-class `Aggregate.grouping_sets`
  payload (index-sets into `group_keys`; DuckDB / Calcite style) rather than
  expanding to a UNION — so the Aggregate output stays the ordinary
  `[keys…, aggregates…]` and all the existing projection / producer / HAVING logic
  is unchanged. Every grouping key is nullable, matching the analyzer's
  `projection_of`. `ROLLUP(a,b)` → `{{0,1},{0},{}}`, `CUBE(a,b)` → all 2ⁿ subsets,
  `GROUPING SETS ((a),(b))` → the listed sets. The analyzer types the `GROUPING()`
  indicator (BigInt, never NULL). The staged plan s-expr serializes `grouping_sets`
  as `:gsets ((…) …)`, and `04_group_by_rollup` / `11_group_by_cube` /
  `12_grouping_sets` now bind, round-trip, and inject.
- **Delivered by:** analyzer `DB25-Semantic-Analyzer#151` (GROUPING typing),
  binder db25-logical-plan#170 (`grouping_sets` payload + lowering), and this
  harness/pin bump.

### ~~G2~~ — Quantified comparison lowering (`x <cmp> ALL|ANY|SOME (subquery)`) — ✅ CLOSED
- **What (was):** the binder rejected `x <cmp> ALL|ANY|SOME (subquery)` with a
  bind error.
- **Resolution:** it lowers to an owned `ExprKind::Subquery` of
  `SubqueryKind::Quantified` — the comparison op rides in `Expr::bin_op` and the
  ALL vs. ANY/SOME sense in `ExprFlagQuantAll` — owning its bound inner plan, so
  a Filter/Project predicate carries it inline (the s-expr renders
  `(subquery :kind quantified :op … :quant all|any)`). No new join node is
  synthesized; a future decorrelation pass can rewrite it to a semi/anti-join.
- **Pinned by:** `26_quantified_all` (binds, round-trips, injects).

### ~~G3~~ — DDL statement lowering (CTAS closed; plain DDL is a catalog op) — ✅ CLOSED
- **CTAS — ✅ CLOSED, end to end.** `CREATE TABLE <name> AS <query>` lowers to a
  `CreateTableAs` logical op over the bound + optimized defining query
  (`table_name` the target, `output` the query's schema, `children[0]` the
  query): the analyzer resolves the defining query (previously a
  `CreateTableStmt` was left unanalyzed), the binder wraps it (pinned by
  `36_create_table_as` — binds, round-trips, injects, mutation-caught), and
  `execute_ddl` REGISTERS the new table, deriving its columns from the query's
  projection so a later statement resolves against it. Increment 3.9a of the
  physical planner completes the path: `36` no longer stops at the logical stage
  but lowers to a physical `CreateTableAs` over the planned query, so the fixture
  is green through every stage rather than through four of five. The `gap_closures` corpus
  session exercises this: the two CTAS create the tables (`exec_ok`), and the
  following `SELECT`s against them analyze clean.
- **Plain `CREATE TABLE (cols)` — not a query plan (by design).** It carries no
  query, so the binder returns an honest error ("plain CREATE TABLE is a catalog
  operation, not a query plan"); it is applied to the catalog via `execute_ddl`,
  which is where its effect lives. Pinned by `31_ddl_constraints`.

### ~~G4~~ — Qualified star over a join — ✅ CLOSED
- **What (was):** `SELECT u.* FROM u JOIN v …` — a qualified star whose relation
  is one side of a join — was reported as a not-yet-lowered arity divergence.
- **Resolution:** `lower_projection` expands a qualified `q.*` to exactly
  relation `q`'s columns (in the analyzer's projection order, USING/NATURAL
  merged-column copy included), so `SELECT u.*` over a join binds to u's columns
  rather than the whole frame. Pinned by `35_qualified_star_join`.

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

### ~~G11~~ — `LATERAL` joins — ✅ CLOSED
- **What (was):** the parser had no `LATERAL`: comma-form `FROM a, LATERAL (subq)`
  was rejected and the JOIN keyword list did not recognize `LATERAL`, so
  `CROSS`/`LEFT JOIN LATERAL` failed to parse. A correlated derived table (a
  reference to a FROM sibling) had no legal spelling.
- **Now:** `LATERAL` parses to a `LateralJoin` node (comma / `CROSS` / `[INNER]` /
  `LEFT` forms), keeping the outer-join qualifier in `primary_text`. The analyzer
  grants the RHS sibling visibility; the binder exposes the left context on
  `outer_inputs_`, so the correlation lowers to depth-1 `OuterRef`s, and the join
  lowers to `JoinType::Lateral` (comma/cross/inner) or `JoinType::LeftLateral`
  (LEFT — RHS also null-extended). `RIGHT`/`FULL`/`NATURAL JOIN LATERAL` are
  rejected (a circular dependency SQL forbids), honest at the parser.
- **Pins:** parser #124/#125, analyzer #157/#158, logical-plan #176/#177; fixtures
  `33_lateral` (cross, `JoinType::Lateral`) and `34_left_join_lateral`
  (`JoinType::LeftLateral`, RHS null-extended), the `lateral` corpus session, and
  the two `showcase.sql` LATERAL queries.

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
