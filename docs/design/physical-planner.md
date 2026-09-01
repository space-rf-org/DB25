# DB25 Physical Planner — Design

**Status:** accepted — decisions D1–D10 settled; one item (fast-path threshold) deferred to tiering · **Scope:** logical plan → physical plan; no execution engine · **Author:** Chiradip Mandal

---

## 1. Why (the one page)

The physical planner is the last stage that is still pure metadata: it turns a
**logical plan** (relational algebra — *what*) into a **physical plan** (an
executable operator DAG — *how*: which join algorithm, which access path, which
data layout, which parallelism, which storage substrate). It emits a plan; it
never runs one.

This is the most load-bearing stage in the frontend, for two reasons. It is
where an HTAP engine earns its name — the row-vs-column, fresh-vs-replica,
seek-vs-scan, local-vs-distributed decisions all live here — and it is where our
time budget is tightest, because a cost-based search is thorough by nature. So we
commit up front to a small number of invariants and let everything else be spec.

We assume the reader knows Cascades, the memo, System-R DP, and branch-and-bound;
this document does not rehearse them. It records **what we chose for DB25 and
why**, and the plan to build it.

**The contract.** The planner is a pure, deterministic function of its declared
inputs:

```
physical_plan  =  plan( logical_plan,
                        catalog_stats,
                        execution_capability_profile,     // structural, static
                        runtime_profile? )                // running, optional
```

Nothing about execution leaks in except through those typed inputs. Purity is the
whole game: it is what keeps the stage golden-testable (a 6th "physical" stage on
the existing staged-golden harness, under the falsifiability gate), and it is what
lets the execution engine arrive *later* without the planner ever calling into a
running system. Feedback is just another input, never a hidden channel.

**The load-bearing invariant.** *One physical IR, one cost model, deterministic
tier selection.* Every plan — however it was produced — is the same IR, costed by
the same model, and which path produced it is a deterministic function of the
query, not a runtime race. Hold this and the planner stays spec-driven and
testable. Drop it and we own two of everything.

**The two provisions for the (future) execution engine.**

- **`ExecutionCapabilityProfile`** — structural, static, *pull*-based. What the
  engine *can* do and what the hardware *is*: the physical operators and variants
  it executes, the encodings it reads, its parallelism and memory model, its
  pushdown surface, SIMD width, NUMA topology. The planner prunes illegal
  alternatives and parameterizes its cost model from this. We ship a **reference
  profile now** — a spec describing a competent hardware-native engine — so the
  planner is fully functional and testable *before any engine exists*.
- **`RuntimeProfile`** — dynamic, running, *push*-based. Observed cardinalities,
  timings, memory high-water, spills, real selectivities. Consumed two ways
  (inter-query: correct estimates for next time; intra-query: adaptive re-plan at
  pipeline boundaries). We define the **type and the seam** this pass and leave
  the *producer* for when the engine lands. The planner cannot tell whether a
  cardinality came from a catalog guess or a real run — same typed input.

**Sequencing.** Cascades first, fast path later — *seams designed in now,
implemented later*. See §3.

---

## 2. Design decisions

Each decision is stated as a choice + the rationale + what it rules out.

### D1 — Cascades, spec-interpreted (not hard-coded)

A memo-based, rule-driven, property-guided, top-down branch-and-bound optimizer,
where the planner *interprets a spec* rather than embedding operator knowledge in
control flow. Adding an operator is a spec edit plus a cost function, never a
planner rewrite.

- **Why:** it is the only optimizer family that makes "absolutely spec-driven"
  honest — rules and properties are declarative data. It is also the family that
  models the HTAP decisions (format, distribution) as *properties with enforcers*
  uniformly, instead of as special cases.
- **Rules out:** System-R bottom-up DP (bakes the search into code — anti-spec)
  and staged-heuristic-only planning (can't cost the row-vs-column choice, which
  *is* the plan in HTAP).

### D2 — The pure-function contract and the two profiles (see §1)

The planner takes `(logical_plan, catalog_stats, capability_profile,
runtime_profile?)` and returns a physical plan. The reference capability profile
ships now; the runtime-profile producer does not.

- **Why:** decoupling from execution without inversion of control. The engine
  "plugs in" by populating inputs — no callbacks into a running system, no
  planner→engine dependency edge.
- **Rules out:** a planner that queries a live engine (couples the two, destroys
  determinism and testability).

### D3 — One physical IR, one cost model

Every tier and every rule emits the same physical IR and is costed by the same
model. The cost model is **hardware-parameterized**: coefficients live in a
**`CalibrationProfile`** (cache-line size, SIMD width, memory bandwidth, NUMA,
per-op throughput) that is a deterministic *input* to the planner, not baked into
code — this is where "hardware-native" meets "spec-driven." Its source is
pluggable (see D10): live-measured, a pinned lab profile, or a cached
host-fingerprinted profile. Because the coefficients are an input, goldens pin a
lab profile and stay deterministic while production adapts to the real host.

- **Why:** the invariant of §1. A second cost intuition anywhere produces plan
  cliffs and non-determinism.
- **Rules out:** a "fast" cost shortcut that disagrees with the searcher's model.

### D4 — HTAP substrate as a physical property, not a special case

Storage format (row / column / delta+main), freshness/consistency (fresh row
store vs. possibly-lagging column replica), and distribution/partitioning are
modeled as **physical properties** with **enforcer operators** (format-convert,
exchange/repartition, sort). Freshness additionally carries a *correctness
constraint*, not only a cost.

- **Why:** this is the TiDB lesson — the row-vs-column choice becomes exactly the
  same machinery as sort-order or distribution, and the planner routes each
  subtree to a substrate by costing enforcers, uniformly.
- **Rules out:** a bespoke "access path picker" bolted beside the optimizer.

### D5 — Cascades first, fast path and tiers as designed-in seams

Build the cost-based search first. Design the tier-entry seams now (plan-shape
cache key, a complexity threshold hook, a budget-guard fallback point) but
implement only Cascades. The fast path (tier 1) and plan cache (tier 0) are a
*later addition, not a later retrofit*.

- **Why:** HTAP's bimodal budget genuinely justifies a µs-class fast path for OLTP
  point ops — but correctness and generality come from the full search, so it goes
  first. The fast path must be a *restriction* of Cascades' decisions (same cost
  model, less search), so crossing the threshold never worsens a plan.
- **Rules out:** two independent planners with a selector (two cost models, two
  bug sets, non-deterministic goldens — the mess we explicitly rejected).

### D6 — The Postgres time envelope as a forcing function (see §3)

The planner is designed against a planning-time budget derived from Postgres's
total planning time on a reference query/host. This is a design constraint, not a
feature — it forces good memo hygiene and, on large join counts, the budget-guard
of D5.

### D7 — Spec artifacts, conformance, and a physical golden stage

Three artifacts live on seams, each independently testable: the capability profile
(planner ↔ engine), the serialized physical plan (planner → engine; **native
format** — see D9), and the stats/metadata provider interface
(where catalog and runtime stats enter, à la Orca's metadata provider). A
**conformance suite** asserts every physical op the planner can emit is declared
executable in the engine profile. Physical plans become a **6th golden stage**
under the existing falsifiability gate (each golden mutation-catchable).

- **Why:** "spec-driven" has to mean the spec is the source of truth *and* is
  tested against, not a doc that drifts.

The spec is an **external, versioned IDL** (not an in-code declarative table): a
single source of truth that also **generates the conformance tests**, so the
operator catalog, implementation rules, property/enforcer rules, cost-model
parameters, and engine capability contract cannot silently diverge from what the
planner and engine actually do. Editing the IDL is how an operator is added; the
generated conformance suite fails if code and spec disagree.

### D10 — Calibration: measured, but sourced pluggably

The `CalibrationProfile` of D3 is produced by a **calibration harness** that
microbenchmarks the host (memory bandwidth, cache latencies, SIMD throughput,
per-op costs). Its source is pluggable, three ways:

- **live** — measure this host at startup/setup;
- **pinned lab** — a checked-in profile for reproducible tests, lab work, and
  deterministic goldens;
- **cached** — a previously-measured profile keyed by a host fingerprint, reused
  to skip re-measurement on a known machine.

- **Why:** coefficients stay a deterministic *input* (preserving the pure-function
  contract and golden reproducibility) while production still adapts to real
  hardware. Testing and lab work never depend on live measurement; a known host
  never re-measures.
- **Rules out:** coefficients hard-coded in the cost model, and any nondeterminism
  leaking from live measurement into the golden pipeline.

### D8 — Where it lives — **resolved: own repo `db25-physical-plan`**

A **separate repo** (`db25-physical-plan`) downstream of `db25-logical-plan`,
pinned into the umbrella — mirroring the decoupling of a standalone optimizer
(Orca). Consumes the logical plan IR; depends on nothing below it except the
shared AST/type vocabulary.

- **Why:** matches the existing multi-repo modular structure and the D2 decoupling
  contract; keeps the physical planner's spec and goldens self-contained.

### D9 — Plan interchange — **resolved: native source of truth; Substrait export is validation-only, off the critical path**

The serialized physical plan handed to DB25's execution engine is a **native
format** — zero-dependency, arena-friendly, expressing DB25's full physical/HTAP
content (format, freshness, substrate, access path, join algorithm, pipeline
structure, cost) with no impedance. It is the single source of truth and the
golden representation.

A **Substrait exporter** exists for **validation only** — projecting the logical
core of a native plan to Substrait so it can run on an external engine
(DuckDB/DataFusion) and have its *results* checked before DB25's own engine
exists. It is **lossy by nature** (all physical/HTAP decisions are dropped —
Substrait cannot express them), so it validates *logical correctness only*, never
DB25's physical choices.

It is **architecturally barred from the execution critical path**, enforced
structurally, not by convention:

1. **One-way dependency edge** — the exporter reads the native IR and projects
   outward (`exporter → native IR`); nothing in the planner core or the engine
   handoff imports it. Deleting it leaves the critical path untouched.
2. **No Substrait/protobuf symbols in the core** — that dependency lives only in
   the exporter's own optional target.
3. **Separate, off-by-default build target** under `validation/` (or `tools/`) —
   a dev/test artifact, excluded from the shipped engine.
4. **Retirement clause** — once the execution engine can validate DB25's own
   physical plans directly, the exporter has served its purpose; it may be kept as
   a dev convenience or removed, and its removal is a non-event.

- **Why:** DB25's only physical-plan consumer is DB25's own engine, so Substrait's
  headline benefit (cross-engine interop) is not a requirement we consume; its
  cost (a general, evolving, protobuf-shaped dependency, plus the distinctive
  physical content forced into private extensions) is exactly the generality tax
  we avoid. Native-first is also the low-regret path: a native→Substrait exporter
  can be added any time; ripping a protobuf core dependency back out cannot.
- **Rules out:** Substrait (or any general interchange) as the production handoff
  format, and any core-path dependency on it.

---

## 3. The time envelope

This is a first-class target, stated honestly.

**Reference measurements (identical host, warm medians, empty tables):**

| Stage | DB25 today |
|---|---|
| tokenize → parse → analyze → bind → optimize (logical) | **23.3 µs** |
| physical planning | 0 (does not exist yet) |
| **Postgres 16 total planning time (fused)** | **~74.0 µs** |

Postgres has **no separately-reportable physical-planning number** — its
~74 µs *fuses* logical optimization, path (physical) selection, and costing. So we
set two targets, not one:

- **Envelope target (primary, defensible).** DB25 total `tokenize → physical plan`
  stays within Postgres total planning time on the reference query/host. Today:
  23.3 µs used, **~51 µs headroom** for physical planning. This is the honesty
  tripwire — if we blow it on the reference query, something is wrong (over-search,
  poor memo hygiene), and the benchmark catches it as a regression.
- **Physical-pure-time (tracked, not claimed).** We allocate a budget from the
  headroom and golden-track physical planning time as a first-class metric,
  reported honestly. We do **not** claim "beating Postgres on physical," because
  that slice does not exist to compare against.

**The tension, named.** Cascades full search is thorough, not cheap. On the
reference query (few joins) it fits the ~51 µs headroom comfortably. On large join
counts it will not — which is exactly why D5's fast path and budget-guard are on
the roadmap. The envelope *is* the reason the roadmap has those, not gold-plating.

**Non-goal, operationalized.** Beating anyone on speed is a non-goal on principle;
correctness, generality, and spec-drivenness win every tie. The envelope's role is
a **regression tripwire and an honesty reference**, kept because a number without
material impact means nothing — so we hold ourselves to a real, reproducible one.

**How the umbrella exercises it.** The staged harness and the integration check
lower through the SHIPPED spec, not the built-in single-candidate fallback, so the
physical goldens pin what the real configuration produces. A spec that stopped
loading, or stopped conforming, fails loudly rather than silently reverting the
harness to a planner nobody runs - which is what made the keyless-MergeJoin defect
visible the moment the wiring landed.

**How we measure.** `stage_timer.cpp` carries a `T6 physical` stage (fresh inputs
per iter, warm median over N runs, same as the others). Physical-plan median on the
reference query is a reported metric, not a hard CI gate — wall-clock gates are
noisy — but a tracked series that flags regressions.

Two things that series has already been wrong about, both recorded here because
the lesson generalizes past this planner:

- **It must lower through the SHIPPED SPEC.** T6 originally called `lower()` with a
  default context, which times the built-in fallback mapping — fewer candidates, no
  spec implementation rules — so every figure it produced described a configuration
  nobody runs, understating the real planner by about 50%. (The same defect hit the
  staged harness in Unit 1.5 and was caught there by a golden; here it survived
  longer, because a timing tool has no golden to disagree with. A number quietly
  measuring the wrong thing still looks like a number.)
- **Only same-machine, same-session comparisons mean anything.** Absolute figures
  from different sessions differ by more than the changes being measured, and a
  regression was once escalated on a cross-session comparison that turned out to be
  machine load. Every timing claim in this document is an A/B measured in one
  session, on one machine, in Release.

Because wall-clock is this fragile, the planner's actual regression GATE is
allocations per `lower()` — deterministic, machine-independent, and the mechanism
behind every performance regression this planner has had.

---

## 4. Implementation plan

Each increment: falsifiability repro before it's called done, per-repo branch off
`origin/main`, regression + golden tests, full suite green under ASan/UBSan, one
PR per concern, merged when CI is green, pins propagated through the chain.

**Increment 0 — the skeleton that exercises every seam.**
- Physical IR (the one IR of D3) + the memo structure (arena-allocated, matching
  DB25's allocator culture).
- Property/enforcer framework: interesting orders + one HTAP property
  (storage-format) with its convert enforcer.
- The external **versioned IDL** (D1/D7) seeded with the operator catalog for the
  ops below, plus the generator that emits the conformance suite from it.
- The reference `ExecutionCapabilityProfile` (in the IDL) + the `RuntimeProfile`
  input type and seam (unpopulated).
- Hardware-parameterized cost model reading a **pinned lab `CalibrationProfile`**
  (D10) for deterministic goldens; the live/cached sources are stubbed behind the
  same seam.
- Lower **scan / filter / project / one join algorithm** end-to-end.
- 6th "physical" golden stage wired into the staged harness + falsifiability gate.
- `stage_timer` T6 stage; record physical median on the reference query.
- *Exit criterion:* the reference query lowers to a physical plan, golden-pinned,
  within the envelope, every seam present (even if the runtime-profile producer is
  absent).

**Increment 1 — the HTAP decision made real. DELIVERED.**
- Multi-candidate lowering: implementation rules became one-to-many, memo groups
  carry their cardinality and what their winner provides, and the per-operator
  cost formulas were extracted so the tree form and the memo form are literally
  the same arithmetic - the "one cost model" invariant (D3) made structural.
- MergeJoin as a real second join candidate: cheaper per row but REQUIRING both
  inputs sorted on the join keys, so a candidate is charged for the enforcers it
  causes and the plan that is built is the plan that was costed.
- Row-vs-column substrate as a costed choice, via a StorageCatalog input
  declaring which formats each table is available in (D4).
- Freshness as a CORRECTNESS constraint rather than a cost preference: a lagging
  copy cannot answer a query that must see the latest writes, no enforcer can
  manufacture freshness, so such a candidate is discarded and - when nothing
  qualifies - the lowering fails honestly.
- Applicability before cost: a candidate whose precondition does not hold is not
  a cheaper option, it is not an option. (MergeJoin needs at least one equi-key.
  Without this the planner emitted a merge join over a cross product, because with
  no keys it also required no sort and so looked cheapest. The umbrella's physical
  golden stage caught it; the unit tests had not, because they only ever joined on
  a key.)

**Increment 2 — search maturity: property-directed group optimization. DELIVERED.**
- A group's winner is keyed by the REQUIREMENT it was optimized for. The old
  single winner - one best plan per group, settled bottom-up before any consumer
  stated a requirement - is the wrong unit of memoization for Cascades: it makes
  it impossible to ASK a group for a property, only to charge it for lacking one.
- Top-down `optimize_group`, splitting lowering into EXPLORATION (groups and
  candidates, no costs) and OPTIMIZATION (the search proper). Per candidate it
  costs two routes and keeps the cheaper: satisfy the operator's own input needs
  and enforce ON TOP, or push the requirement INTO the input where the operator
  preserves it. Neither is universally better - a Sort below a Filter sorts rows
  the Filter is about to discard - so it is a cost comparison, not a rule.
  The motivating case works: the same join takes a HashJoin unconstrained and a
  MergeJoin when its output is required in key order.
- Branch-and-bound, with an ADMISSIBLE bound: every cost term is non-negative, so
  a partial cost is a lower bound on the total and a pruned candidate cannot have
  won. The gate is not that pruning is fast but that it changes NOTHING - every
  case is lowered twice, pruned and exhaustive, and the plans must be identical.
- Structural memo dedup, OPT-IN. Measured at +1.0us to find nothing: no query in
  the corpus repeats a logical subtree. Its premise is rule application - join
  reordering above all - so it ships complete, tested, and off until then.
- The D5 budget guard, wired to a spec-declared join-count threshold and REPORTED
  rather than silent. Open question 5 stands: the threshold's VALUE is still not
  derived from the planning-time budget, only made into data.

*What this increment cost, and what that taught.* Unit 2.1 nearly doubled T6 while
changing no plans, and was merged because NOTHING MEASURED IT - T6 lived in this
document, not in a build. The root cause was older: Increment 0 above specifies an
arena-allocated memo and shipped std::vector and std::string instead, a deviation
that was free when made and compounding by the time anyone noticed (116 heap
allocations per lower() for a five-node plan). The repair removed allocations
rather than relocating them - operand-indexed data is arity-bounded and held
inline, the group's schema is borrowed - and added an ALLOCATION BUDGET test,
because allocation counts are deterministic and machine-independent where
wall-clock ceilings are neither.

*Cost, corrected.* The figures first recorded here (T6 2.51 -> 2.89us) came from a
`stage_timer` that lowered with a default context and so timed the FALLBACK
mapping, not the planner as shipped. Measured through the shipped spec, same
machine, same session:

| pin | T6 | parse -> physical |
| --- | --- | --- |
| pre-Increment-2 | 3.59 us | 11.81 us |
| Increment 2 as first merged | 4.91 us | 13.13 us |
| after five optimization passes | 4.15 us | 12.14 us |

So the increment cost +37% before optimization and +16% after, against a figure
that had been reported as +15%. It remains comfortably inside the ~51 us of
headroom, but the earlier number was measuring the wrong planner, and a design
document that records the flattering measurement is worse than one that records
none.

Two tests in this increment asserted the wrong thing and were caught only by
mutating the code they covered: one blessed the defect it was written to prevent,
one checked that a flag was reported rather than that the guard bounded anything.
Falsification is not a formality here; it is the step that finds these.

**Increment 3 — parallelism & distribution.**
- Pipeline/pipeline-breaker identification (codegen-ready shape, HyPer lesson).
- Exchange/repartition enforcers; distribution as a property.

**Increment 4+ — fast path & plan cache (tier 0/1).**
- Plan-shape cache key (parameters separate).
- Heuristic fast path as a *restriction* of the Cascades rule set (one catalog,
  fixed non-cost application strategy), below a spec-declared complexity threshold,
  golden-tested at the boundary.
- Adaptive-execution `RuntimeProfile` consumer — the first real producer, if the
  execution engine has begun by then.

**Validation track (parallel, off the critical path).**
- The **lossy Substrait exporter** (D9) — an optional `validation/` target that
  projects a native plan's logical core to Substrait and runs it on an external
  engine to check *results*. Built when early logical-correctness validation is
  wanted; never linked into the planner core or the engine handoff; retired once
  DB25's own engine can self-validate physical plans.

---

## 5. Open questions

1. ~~**Location (D8):** own repo vs. module.~~ **Resolved: own repo
   `db25-physical-plan`.**
2. ~~**Plan interchange:** Substrait vs. native.~~ **Resolved (D9): native source
   of truth; Substrait export validation-only, off the critical path.**
3. ~~**Spec literalness.**~~ **Resolved: external versioned IDL that generates the
   conformance tests (D1/D7).**
4. ~~**Cost-model calibration.**~~ **Resolved: calibration harness measuring the
   host, with a pluggable source — live / pinned-lab / cached (D10).**
5. **Threshold derivation (D5):** is the fast-path complexity threshold derived
   from the planning-time budget, and how is it golden-pinned at the boundary?
   *Deferred — decide when we reach tiering.*

---

## Reference systems (rationale, not rehash)

- **HyPer → Umbra (TU Munich)** — unified engine, morsel-driven parallelism,
  data-centric codegen, adaptive compilation. Lesson: physical plan = pipelines
  with explicit breakers; adaptive compilation is our first `RuntimeProfile` use.
- **SingleStore** — universal (row+column) storage, compiled plans, plan cache on
  parameterized shape, data-movement enforcers, segment elimination. Lesson: plan
  *shape* as cache/golden key; movement/convert as first-class enforcers.
- **TiDB + TiFlash** — row (TiKV) + columnar (TiFlash Raft-learner) stores; a
  cost-based optimizer routes each subtree to a substrate; stats/plan management.
  Lesson: substrate/format as a physical property (D4).
- **Orca (Greenplum)** — the decoupled, spec-driven standalone-optimizer template
  (DXL interchange, metadata-provider interface). Lesson: D2 + D7.
- **SQL Server** — Adaptive Query Processing + Query Store: the canonical
  running-profile feedback loop. Lesson: the shape of the `RuntimeProfile` seam.

*Architecture facts above are from training knowledge (cutoff Jan 2026);
version-specific mechanisms (e.g. TiDB statistics feedback) have shifted across
releases and should be re-verified before any is cited as current.*
