# Formal Methods in DB25 — Where TLA+ and Alloy Fit (Proposal)

**Status:** proposal / RFC
**Date:** 2026-07-21
**Scope:** the whole DB25 engine lifecycle — the logical layer that exists today and
the physical / execution / storage layers that don't yet.

---

## 0. TL;DR

- DB25 has **no** formal specifications today (verified: no `.tla`/`.als`/TLC configs
  anywhere in the org). That is currently the **right** default.
- Formal methods (TLA+ / Alloy) earn their keep on **non-deterministic, stateful,
  failure-prone** subsystems. DB25 today is a **deterministic, single-threaded,
  read-only query compiler** — it has almost none of that surface area, so broad
  adoption now would largely re-prove what differential + property testing already
  covers.
- **Recommendation:** do *not* adopt a standing formal-methods methodology for the
  logical layer. Instead:
  1. **Now (optional, high-clarity spike):** one **Alloy** model of *subquery
     decorrelation equivalence* under bag + three-valued (NULL) semantics — the
     riskiest existing rewrite — and a small **Alloy/TLA+** invariant for the
     *positional-column remap*. Treat these as targeted design-verification spikes.
  2. **Bank for later (the real payoff):** when DB25 grows an executor with
     parallelism, and especially a storage/transaction layer with MVCC / WAL /
     recovery, **write the TLA+ spec of the isolation / recovery protocol *before*
     implementing it.** That is the proven, outsized database payoff.
- **Complement, don't replace:** the existing adversarial-review + differential-test
  approach — and a proposed **deterministic simulation fuzzer** (FoundationDB
  Flow/`BUGGIFY` style) — cover the deterministic layers better and cheaper than a
  model checker. Formal methods are for the parts those techniques *can't* reach:
  concurrency, isolation, crash recovery.

---

## 1. What each tool is good at (so we apply the right one)

| | **Alloy** | **TLA+ / PlusCal** |
|---|---|---|
| Models | Relational / structural: sets, relations, constraints over a *snapshot* structure | Behavioral / temporal: state machines, sequences of states, safety **and** liveness |
| Checks | Bounded exhaustive search for a counterexample within a finite scope | Model checking (TLC) or symbolic (Apalache) over reachable states |
| Sweet spot | "Is property P true of *all structures* up to size N?" — data-model invariants, **equivalence of two relational expressions**, referential integrity, schema constraints | "Does this *protocol* preserve invariant I across all interleavings and failures?" — concurrency, commit, replication, recovery |
| DB25 fit | **Rewrite equivalence, IR structural invariants** (logical layer, now) | **MVCC / isolation / WAL / recovery / any concurrent scheduler** (storage & executor, later) |

Decision rule for DB25: **structural / relational-equivalence question → Alloy;
temporal / concurrent-protocol question → TLA+.**

---

## 2. Mapping to the DB25 architecture & lifecycle

DB25 is built one repo per stage. Formal-methods value is **not** uniform across
them — it tracks *non-determinism and shared mutable state*, which appear only in the
later layers.

| Layer | Exists? | Non-determinism / shared state | Formal-methods value | Tool |
|-------|---------|-------------------------------|----------------------|------|
| Tokenizer, Parser, Analyzer | yes | none (pure, deterministic) | ~none — differential/fuzz testing dominates | — |
| **Binder + Logical optimizer** | yes | none (pure rewrites) | **low–moderate, targeted** — rewrite equivalence, remap invariants | **Alloy** (small) |
| Physical planner | next | none (still deterministic) | low — property-level invariants suffice | Alloy / property tests |
| Executor + parallelism | later | **concurrency** (once morsel-driven / multi-threaded) | **moderate–high** — scheduler / operator-state invariants | TLA+ + simulation |
| Storage + transactions | much later | **MVCC, WAL, crash recovery, (maybe) distribution** | **high — canonical payoff** | **TLA+** (spec before code) |

**The core insight:** the highest-leverage use of TLA+ (isolation & recovery
protocols) has **no surface area in DB25 yet**. Introducing it now would be tooling in
search of a problem. The value grows precisely as DB25 grows state and concurrency.

---

## 3. Concrete first targets

### T1 — Alloy: subquery-decorrelation equivalence (bag + NULL semantics) — *optional, now*

**Why.** Decorrelation (EXISTS/IN/correlated-scalar → semi/anti/left joins;
correlated-aggregate-scalar → LEFT JOIN + GROUP BY) is DB25's **riskiest** rewrite:
the bugs live exactly where a model checker shines — *bag* multiplicity (row
duplication / cardinality), *three-valued* NULL logic (NOT IN, anti-join,
LEFT-JOIN-NULL vs. aggregate-over-empty), and *empty-relation* edge cases. These are
the failure modes the adversarial reviews kept probing by hand.

**Property.** For a bounded universe of tuples, model the *input* relations and both
the *correlated* form and the *decorrelated* (join) form as relational expressions;
assert the two produce the **same output multiset** under bag semantics and SQL
three-valued logic. Search for a counterexample within a small scope (≤ ~4 rows/attrs).

**Model sketch.** Signatures for `Tuple`, `Value` (with an explicit `NULL` atom),
`Relation` as a bag (tuple → count). Predicates for `semijoin`, `antijoin`,
`leftJoinGroupBy`, and the correlated evaluation. A `check` that the multiset outputs
are equal, over `for 4 Tuple, 3 Value`.

**Effort / payoff.** ~1–2 focused days per shape. Payoff is *design clarity and
edge-case coverage* (it will either corroborate the hand proofs or surface a scope
we don't handle), **not** a safety-critical guarantee. Worth doing once, for the
scalar/anti-join shapes; keep the existing differential tests as the primary oracle.

**Bonus:** this model becomes the specification for the *general* decorrelation
refactor (the Neumann–Kemper dependent-join direction): the Alloy equivalence check is
the oracle a general algorithm must satisfy.

### T2 — Alloy (or tiny TLA+): the positional-column remap invariant — *optional, now*

**Why.** The entire optimizer manipulates **positional** column indices. Pruning,
pushdown, and decorrelation each remap `#i` slots across `left ++ right` frames. The
invariant "every `ColumnRef.input_index` remains a valid, stable reference to its
intended source column after a rewrite" is exactly the class of bug that surfaced
(e.g. the semi-join `pred_remap` vs `out_remap` distinction, the `new_lw + rrm[j]`
rebasing).

**Property.** Model a plan node's input/output schemas and a remap as a partial
function; assert each rewrite's remap is (a) index-preserving for kept columns,
(b) never dangling, and (c) composes associatively down a chain of operators.

**Tool.** Alloy fits (it's structural), but this is small enough that a
property-based test in C++ (random schemas + random remaps + the real
`remap_expr_slots`/`apply_output_remap`) captures ~the same assurance at lower cost.
**Recommendation:** do this as a **property test first**; only lift to Alloy if the
invariant needs to be stated once, canonically, as the contract for all future passes.

### T3 — TLA+: MVCC snapshot isolation / WAL recovery — *deferred; bank the intent*

**Why.** This is the canonical, industry-proven database use of TLA+ (AWS DynamoDB/S3,
MongoDB & CockroachDB replication/commit, FoundationDB's model-checked core). The
moment DB25 has a transactional storage engine, the isolation level and the
crash-recovery protocol are **exactly** where interleavings and failures produce bugs
that no amount of example-based testing reliably finds.

**Property (when the time comes).** Specify the concurrency-control protocol
(read/commit versions, conflict detection) and assert the chosen **isolation
guarantee** (e.g. snapshot isolation / serializability) holds over all interleavings;
specify WAL + recovery and assert **durability + atomicity across crashes** at every
crash point.

**Discipline:** write the spec **before** the implementation, and keep the
implementation a refinement of it. Do **not** start this until a storage/transaction
repo exists — it has no meaning before then.

---

## 4. Relationship to the existing (and proposed) testing approach

Formal methods are **one column** in a portfolio, not a replacement. The right tool
depends on the layer's determinism:

| Technique | What it gives | Best layer | Status in DB25 |
|-----------|---------------|------------|----------------|
| Adversarial review | Expert refutation of a specific rewrite | logical (now) | **in use** |
| Differential / property tests | Known properties on chosen/random inputs | all deterministic layers | **in use** |
| **Deterministic simulation fuzzer** (Flow/`BUGGIFY` style) | Reproducible counterexamples over *input × fault* space; finds **cross-pass interactions** | whole pipeline now; executor/storage later | **proposed** (see FoundationDB findings) |
| **Alloy** | Exhaustive structural / equivalence proof in bounded scope | logical rewrite equivalence, IR invariants | **this proposal, targeted** |
| **TLA+** | Safety + liveness over all interleavings & failures | concurrency, isolation, recovery | **this proposal, deferred to storage/txn** |

**Why simulation testing, not TLA+, is the primary tool for the deterministic
layers:** DB25 is *already* deterministic, single-threaded, arena-based, and
exception-free — the exact conditions FoundationDB's Flow spends enormous effort
engineering *into*. A seeded fuzzer that runs the full pipeline and checks pre/post
rewrite semantic equivalence (plus IR invariants) searches a far larger space than a
bounded model checker and reduces every failure to a replayable seed — at a fraction
of the specification cost. TLA+/Alloy are reserved for what simulation *can't* prove
exhaustively (equivalence in T1) or for state spaces it can't practically reach
(concurrent isolation/recovery in T3).

---

## 5. Recommendation & sequencing

1. **Now:** keep the current approach for the logical layer. Optionally run **T1**
   (Alloy decorrelation-equivalence) once as a design-verification spike for the
   scalar / anti-join shapes, and do **T2** as a C++ property test. Neither becomes a
   standing gate.
2. **Alongside the physical planner / executor:** invest in the **deterministic
   simulation fuzzer** (bigger ROI than any spec at this stage). Introduce TLA+ for
   the executor's scheduler **only if** it becomes genuinely concurrent.
3. **At the storage / transaction layer (highest value):** adopt TLA+ as a
   first-class practice — **spec the isolation and recovery protocols before writing
   them** (T3). This is where formal methods have their proven, outsized database
   payoff, and it is worth doing properly.

**One-line policy:** *Model-check protocols, not functions.* DB25 has functions today
and will have protocols later; introduce TLA+/Alloy exactly when the protocols appear.

---

## 6. Where the specs and this proposal should live (repo strategy)

- **This proposal** (cross-cutting methodology): conceptually belongs in the umbrella
  **`DB25`** repo, but that repo is dormant; pragmatically it lives in
  **`db25-logical-plan/docs/`** (active, and co-located with the only near-term
  target, T1). Link it from the umbrella repo when that is next touched.
- **The specs themselves live beside the code they verify** — specs rot in isolation:
  - **T1 / T2** (decorrelation equivalence, remap invariants) → **`db25-logical-plan`**
    (e.g. a top-level `formal/` directory), next to the optimizer.
  - **T3** (MVCC / WAL / recovery) → the **storage / transaction repo, when it
    exists** — never retrofitted into the logical repo.
- **Tooling:** Alloy Analyzer for Alloy; TLC (exhaustive) and/or Apalache (symbolic,
  scales better) for TLA+. Keep specs out of the C++ build; check them in CI as a
  separate, non-blocking job until a layer makes them load-bearing.
