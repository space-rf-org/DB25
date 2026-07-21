# DB25 End-to-End Test Harness — First-Run Findings

**Date:** 2026-07-21
**Harness state:** builds clean; `db25_harness` = **75 passed / 5 failed / 80 total**;
`db25_gate` (falsifiability gate) catches all 5 mutants and reports the 5 clean
failures plus 18 non-falsifiable (vacuous) tests below.

The failures are the point: on its first run the harness surfaced **four genuine
db25-logical-plan bugs** (verified independently, not harness artifacts) plus one
model mismatch. Each is a tracked follow-up against `db25-logical-plan`; the harness
tests stay in the suite as living repros.

---

## Real bugs found in `db25-logical-plan`

### 1. Self-join / same-table alias column resolution (CORRECTNESS — high)
`SELECT e.id, m.id FROM emp e JOIN emp m ON e.mgr_id = m.id` binds to:
```
Project (#0, #0)                     <- both e.id AND m.id resolve to #0 (the LEFT scan)
  Join (INNER) ON (#2 = #0)          <- ON is e.mgr_id = e.id, both from the left
    Scan emp AS e [...]
    Scan emp AS m [...]              <- the right alias's columns are never referenced
```
When the same base table is joined to itself, the second alias's column references
resolve into the *first* scan. Both the bound and optimized plans are equally wrong,
so a naive bound-vs-optimized differential check passes — the harness catches it with
an independent JOIN/IN cross-check. Caught by `join.self_join_on_unique_key`.

### 2. `JOIN … USING(col)` lowered as a cross join (CORRECTNESS)
A `USING` join produces an INNER join with **no predicate and no column merge** — it
behaves like a cross product instead of an equi-join on the named column(s). Caught by
`join.using_compacts_frame`.

### 3. `NATURAL JOIN` drops the second relation (CORRECTNESS)
A `NATURAL JOIN` lowering loses the right relation entirely. Caught by
`join.natural_equals_using`.

### 4. `optimize()` is not idempotent (ROBUSTNESS)
For queries over stacked joins, `optimize(optimize(p))` differs from `optimize(p)`:
a filter that can be pushed all the way to a base scan is only *partially* pushed in a
single pass and settles between two joins; a second `optimize()` pushes it the rest of
the way. Results are identical (the differential oracle passes), but the plan is not a
fixpoint — predicate pushdown does not fully converge through nested joins in one pass
(the INNER-join pushdown does not recurse into the filter it just pushed, unlike the
semi/anti-join pushdown, which does). ~24 fuzzer seeds hit this. Caught by
`property/optimizer-idempotence`.

---

## Model mismatch (harness-side / design smell)

### 5. Aggregate output model
`property/no-crash-and-ir-well-formedness` asserts the conventional invariant
"Aggregate output width == group_keys + aggregates". DB25's Aggregate output is instead
**SELECT-list-shaped** (plus hidden HAVING columns), so the invariant fails on grouped
queries. This is the same non-standard model behind the known HAVING duplicate-compute
follow-up. Resolution is a choice: relax the harness invariant to DB25's model, or move
DB25 toward the conventional `group_keys ++ aggregates` output. Tracked with the
aggregate-model cleanup.

---

## Non-falsifiable (vacuous) tests — harness hygiene

The gate flags 18 tests that pass but cannot be made to fail by any current mutant —
i.e. they are not yet earning their place. Most are either differential checks the
reference evaluator currently **skips** (unsupported op → `nullopt`, so the assertion is
vacuous) or assertions no mutant exercises. They must be strengthened (add an
expected-value or a mutant that targets them) or removed:

```
card.distinct_equals_group_by            card.duplicates_survive_without_distinct
card.scalar_subquery_preserves_row_count fold.int64_overflow_not_miscomputed
fold.true_or_predicate_keeps_all         join.cross_equals_join_on_true
join.left_join_preserves_all_left        nulls.count_nullable_vs_count_star
nulls.case_null_branch_equals_coalesce   nulls.not_in_empty_subquery_keeps_all
subq.scalar_in_select_with_arithmetic    decorrelation: correlated scalar COUNT must NOT decorrelate
metamorphic: no-op derived table (skips) property/type-nullability-preservation
property/pass-safety/fold_constants      property/pass-safety/simplify_booleans
property/pass-safety/push_down_filters   property/pass-safety/prune_columns
```

Adding a few targeted mutants (e.g. "drop a group key", "corrupt nullability", "make a
pass a no-op individually") would give several of these teeth immediately.

---

## How to run

```
cmake -S . -B build && cmake --build build -j
./build/db25_harness            # run all suites (env: DB25_PROP_SEEDS, DB25_TEST_FILTER)
./build/db25_gate               # falsifiability gate: clean pass + every mutant caught + no vacuous test
DB25_MUTANT=3 ./build/db25_harness   # run under a specific injected defect
```

Reproduce a property failure: each failure prints `seed=<N> sql=[...]`; re-run with
`DB25_PROP_SEED=<N>`.
