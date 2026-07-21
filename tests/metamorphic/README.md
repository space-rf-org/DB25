# Metamorphic tests & the decorrelation oracle

This suite proves the DB25 optimizer's rewrites are **semantics-preserving** by
*evaluating* plans, not by inspecting them. Every assertion compares real result
multisets with `bag_equal`, so a wrong rewrite makes an assertion FALSE — the
tests are falsifiable by construction.

Two files:

- `metamorphic.cpp` — pairs of queries related by a result-preserving
  transformation; asserts the optimized plans are bag-equal (and each optimized
  plan equals its own bound plan).
- `decorrelation_oracle.cpp` — the sharpest falsifier for the marquee feature:
  for a battery of correlated queries it evaluates the **decorrelated** optimized
  plan and, independently, the **correlated** bound plan, and asserts they agree.

## Harness contract / assumptions

- Built against `db25::harness` (`run`, `eval`, `bag_equal`, `test`, `check`,
  `catalog()`, `data()`, `Stages`/`Table`/`Data`). The header is included as
  `../../harness/harness.hpp` (repo-root `harness/`). Adjust that include if the
  core agent publishes the header at a different path.
- **Schema**: the tests assume `catalog()`/`data()` provide the codebase's
  conventional tables
  - `users(id INT NOT NULL, name VARCHAR NULL)`
  - `orders(id INT NOT NULL, user_id INT NULL, total DOUBLE NULL)`
- **Data content** (required for the assertions to be *falsifiable*, not just
  true): `data()` should be non-empty and, for the pessimistic decorrelation
  paths, contain
  - a user with **no** matching orders (EXISTS→false; scalar aggregate→NULL),
  - an `orders` row with `user_id = NULL` (NOT-IN whole-result UNKNOWN, and the
    NULL right key that positive IN/EXISTS must drop),
  - a user with **≥ 2** matching orders (semi/anti join must **not** multiply the
    outer row — the cardinality-preservation proof).

When `eval` cannot evaluate a plan shape (returns `nullopt`), the test asserts
only pipeline success + structural sanity and leaves a `// TODO(oracle)` marker.
A skipped equality is **never** reported as verified.

## Metamorphic relations (`metamorphic.cpp`)

| # | Relation | Example pair | Exercises |
|---|----------|--------------|-----------|
| 1 | Neutral predicate | `WHERE p` == `WHERE p AND 1=1` / `p AND TRUE` / `p OR FALSE` | constant fold + boolean simplify |
| 2 | Double negation | `WHERE NOT (NOT p)` == `WHERE p` | `NOT(NOT x)->x` simplification |
| 3 | Commutativity | `a AND b` == `b AND a`; `u.id=o.uid` == `o.uid=u.id` (join ON) | operand order invariance |
| 4 | Constant collapse | `WHERE 1=1` == no filter; `WHERE 1=0` == empty | fold to `true`/`false` |
| 5 | No-op derived table | `SELECT id FROM (SELECT id FROM users) t` == `SELECT id FROM users` | derived-table / redundant projection (skips gracefully if the binder lacks derived tables) |
| 6 | IN-list vs OR-chain | `x IN (1,2,3)` == `x=1 OR x=2 OR x=3` | IN lowering (positive form) |

For each pair we assert three things: `optimized(q1) == optimized(q2)` (the
relation), and `optimized(qi) == bound(qi)` for each side (the optimizer
preserved each query's own result).

### 3VL (three-valued-logic) caveats

- **Relation 4 falsifiability** depends on `data()` being non-empty. `WHERE 1=1`
  vs *no filter* only distinguishes a broken fold from a correct one when there
  is at least one row; likewise `WHERE 1=0` vs `WHERE id <> id` (which is cleanly
  empty because `users.id` is `NOT NULL`, so no UNKNOWN row leaks through).
- **Relation 6 — the IN-vs-OR NULL subtlety.** Positive `x IN (1,2,3)` and
  `x=1 OR x=2 OR x=3` are equivalent under 3VL: both have the identical truth
  table, and when `x IS NULL` both evaluate to **UNKNOWN**, which a `WHERE` drops.
  So the equivalence holds even though `orders.user_id` is nullable. This does
  **not** extend to the negated form: `x NOT IN (1,2,NULL)` is **not** equivalent
  to `x<>1 AND x<>2 AND x<>NULL` in the way one might naively expect — with a NULL
  in the list, `NOT IN` is UNKNOWN for every probe and the result is empty. We
  therefore only assert the **positive** IN⇔OR relation here; the negated NULL
  behaviour is covered precisely in the decorrelation oracle (NOT-IN cases).

## Decorrelation oracle (`decorrelation_oracle.cpp`)

For each correlated query:

1. **(i)** evaluate the **optimized** plan — the decorrelated Semi / Anti /
   Left-join form.
2. **(ii)** evaluate the **bound** plan — which still holds the represented
   correlated `Subquery`, so the reference evaluator's nested-loop + `OuterRef`
   machinery computes the *true* correlated answer through a code path disjoint
   from the optimizer's rewrite.
3. assert `bag_equal((i), (ii))`.

| Case | SQL shape | Expected rewrite | Independent oracle |
|------|-----------|------------------|--------------------|
| EXISTS (correlated) | `WHERE EXISTS (… o.user_id=u.id)` | SemiJoin | bound-eval **+** cross-check vs `JOIN + DISTINCT` |
| NOT EXISTS (correlated) | `WHERE NOT EXISTS (…)` | AntiJoin | bound-eval **+** cross-check vs `EXCEPT / JOIN` |
| IN (uncorrelated) | `WHERE id IN (SELECT user_id FROM orders)` | SemiJoin (IN eq) | bound-eval **+** cross-check vs `JOIN + DISTINCT` |
| IN (correlated) | `WHERE id IN (SELECT o.id … WHERE o.user_id=u.id)` | SemiJoin (IN eq ∧ correlation) | bound-eval |
| NOT IN (nullable) | `WHERE id NOT IN (SELECT user_id FROM orders)` | **left represented** (no AntiJoin) | bound-eval **+** empty-result cross-check |
| NOT IN (not-null) | `WHERE id NOT IN (SELECT id FROM orders)` | AntiJoin | bound-eval **+** cross-check vs `EXCEPT / JOIN` |
| Scalar SUM/MIN/MAX/AVG (correlated) | `SELECT u.id, (SELECT SUM(o.total) … WHERE o.user_id=u.id)` | LEFT JOIN + grouped Aggregate | bound-eval |
| Scalar COUNT (correlated) | `SELECT u.id, (SELECT COUNT(*) … WHERE o.user_id=u.id)` | **left represented** (must NOT become a LEFT-join) | bound-eval |

### Which oracle is used where

- **EXISTS / NOT EXISTS / IN / NOT IN** — primary oracle is **bound-plan eval**
  (ii), plus an *extra-independent* cross-check against a hand-written SQL
  formulation (`INNER JOIN + DISTINCT`, or `EXCEPT`) that a human can verify by
  inspection and that **never runs the decorrelation pass**.
- **Scalar SUM/MIN/MAX/AVG and COUNT** — oracle is **bound-plan eval**. There is
  no decorrelation-free SQL rewrite of a per-row scalar other than the exact
  LEFT-JOIN+GROUP-BY form the optimizer itself emits, so the represented-subquery
  nested loop in the bound plan is the correct independent reference.
- **Plain-C++ nested loops over `data()`** are *not* used: the contract exposes
  `Table`/`Data` only opaquely (no row/column accessors are specified), so a
  C++ loop can't be written against the contract without guessing internal
  layout. Bound-plan eval is the contract-sanctioned independent oracle and the
  SQL cross-checks are the second, decorrelation-free layer. `// TODO(oracle)`:
  add anchor-case C++ loops if the harness later exposes `Data`/`Table`
  accessors.

### Pessimistic cases the oracle pins down (the 3VL / cardinality traps)

- **Outer row whose correlation match is empty** — EXISTS/NOT-EXISTS over a user
  with no orders (EXISTS→false, so the row drops / survives correctly), and the
  scalar aggregate over the empty set → **NULL**, which the LEFT-JOIN miss
  reproduces. This is why only SUM/MIN/MAX/AVG (NULL over ∅) are decorrelated to
  a LEFT join and **COUNT is not**: `COUNT(*)` over ∅ is **0**, not NULL, so a
  LEFT-join miss (NULL) would be wrong. The COUNT test asserts it stays a
  represented subquery and that `optimized == bound` still holds (both yield 0).
- **NOT IN where the inner set contains NULL** — every probe is UNKNOWN, so the
  whole result is empty. The optimizer keeps this as a represented subquery
  (an AntiJoin would be wrong); the test asserts *no* AntiJoin, that a subquery
  remains, and that the result is empty (cross-checked against a provably-empty
  query). Falsifiable exactly when `data()` has a NULL `user_id`.
- **Duplicate / multi-match outer rows** — a user matching several orders must
  appear **once** in a SemiJoin (and the AntiJoin/Semi form must not multiply the
  outer cardinality). The bound-plan EXISTS/IN also emits the outer row once, so
  any multiplication in the join form is caught by `bag_equal`. Exercised by the
  EXISTS and IN cases whenever a user has ≥ 2 orders.

## Falsifiability

Every equality here is a comparison of real evaluated results, so each can fail:
a broken fold, a wrong simplification, a mis-hoisted correlation predicate, or an
unsound NOT-IN → AntiJoin rewrite all flip a `bag_equal` to false. Where a plan
shape is not yet evaluable, the gap is marked `TODO(oracle)` and never counted as
a verified relation.
