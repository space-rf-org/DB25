# DB25 property-based + fuzzing suite

Deterministic, seed-driven property tests for the DB25 SQL engine's
bind → optimize stack. Every generated query is a **pure function of an integer
seed** (FoundationDB style): the same seed reproduces byte-identical SQL on any
machine, so any failure replays exactly.

## Files

- `sql_generator.hpp` — a seed-driven generator of random-but-valid SQL over the
  harness catalog (`users` / `orders` / `products` / `emp`). Grammar-guided so it
  analyzes cleanly most of the time, and biased toward the interesting cases
  (lots of NULLs, correlated subqueries, decorrelatable shapes). The only source
  of entropy is a hand-written splitmix64 PRNG seeded solely by the integer seed
  — **no `std::random_device`, no clock**.
- `properties.cpp` — the properties, each a registered harness `test()` that
  sweeps seeds `0..N` and `check()`s an invariant.

## Properties

1. **No-crash / IR well-formedness** — every cleanly-analyzing query binds and
   optimizes without crashing, and the optimized plan satisfies IR invariants:
   every `ColumnRef.input_index` is in range for its operator's input frame, each
   operator's output width is consistent with its kind (Project == expr count;
   Semi/Anti join == left width; ON-join == left++right; Aggregate == keys +
   aggregates; Window == child + window funcs; Set-op branches share width), and
   there is no dangling `OuterRef` (depth accounting, scope stack).
2. **Type / nullability preservation** — the optimized root output schema (column
   count, per-column type and nullability) equals the bound root schema. The
   optimizer never changes the result shape.
3. **Optimizer idempotence** — `dump_plan(optimize(optimize(bind)))` ==
   `dump_plan(optimize(bind))`; a second pass changes nothing.
4. **Differential equivalence (centerpiece)** — where the reference evaluator
   `eval` supports both plans, `bag_equal(eval(bound), eval(optimized))`: the
   optimizer preserves the result multiset. The suite counts how often the oracle
   *applies* vs is *skipped* (unsupported op) and prints the coverage. A skip is
   never counted as a pass.
5. **Individual-pass safety** — each pass (`fold_constants`,
   `simplify_booleans`, `push_down_filters`, `prune_columns`,
   `decorrelate_exists`) is applied in isolation and must preserve
   eval-equivalence where checkable.

## Tuning the sweep

- `DB25_PROP_SEEDS=<N>` — number of seeds swept per property (default **3000**).

```sh
DB25_PROP_SEEDS=50000 <test-binary>     # a deeper overnight sweep
DB25_PROP_SEEDS=200   <test-binary>     # a quick smoke run
```

## Reproducing a failing seed

Every `check()` failure message includes the seed and the exact SQL, e.g.:

```
seed=1734 sql=[SELECT t0.id ... ] : optimizer changed the result multiset
```

Re-run the suite pinned to just that seed:

```sh
DB25_PROP_SEED=1734 <test-binary>
```

`DB25_PROP_SEED` makes the suite sweep **only** that one seed across every
property, regenerating the identical query, so the failure is isolated and
replayable. To inspect the query directly, `generate_query(catalog(), 1734)`
returns exactly the SQL that was tested.

## Determinism guarantees

- Query text is a pure function of the seed (`generate_query(cat, seed)`); the
  per-seed "complexity" knob is itself `seed % 4`.
- The generator reads the catalog's real columns/types at construction, so it
  stays valid if the catalog's columns change — but for a fixed catalog, a seed
  always yields the same SQL.
- No global mutable state, no threads, no time or address hashing anywhere in the
  entropy path.
