# DB25 vs. FoundationDB, DuckDB, Polars, SQLite — Comparison & Inspiration

**Date:** 2026-07-21
**Method:** four parallel research passes (real web research for the external systems;
direct repo inspection for the formal-specs audit), each producing a comparison and a
ranked, actionable "inspiration to take" list scoped to DB25's roadmap.

**DB25 today (the baseline for every comparison):** a from-scratch SQL engine built one
repo per stage — SIMD tokenizer → parser (arena-allocated 128-byte AST nodes) →
semantic analyzer → binder → logical optimizer. C++23, `-fno-exceptions`, single-
threaded, deterministic. Owned typed `Expr` IR with **positional** column references and
baked type/nullability. Optimizer passes: constant folding, boolean simplification,
predicate pushdown, column pruning, and subquery **decorrelation** (EXISTS/IN/correlated-
scalar → semi/anti/left joins; correlated-aggregate-scalar → LEFT JOIN + GROUP BY). Each
rewrite gated by adversarial review + differential tests. **No physical planner,
executor, storage engine, transactions, MVCC, or parallelism yet** — the pipeline stops
at an optimized logical plan.

---

## 0. The through-line

Across three very different engines, the findings converge:

- **DB25 is genuinely ahead on IR/representation.** Its owned typed `Expr` IR with
  positional column refs and baked type/nullability is exactly what DuckDB
  (`(table_idx, col_idx)` bindings) and Polars (arena node-ids) *converge toward*, and it
  is richer than anything SQLite keeps internally. Keep it.
- **DB25's decorrelation-into-semi/anti-joins is ahead of SQLite** (which evaluates
  correlated subqueries by repeated per-row execution) and is a hand-specialized subset of
  **DuckDB's general** Neumann–Kemper unnesting. Reasonable subset, not a dead-end — but
  the general dependent-join framing is the direction of travel.
- **The biggest opportunities are below the logical layer**, and three themes recur:
  1. **Testing rigor** — coverage → MC/DC + structured fuzzing + fault injection
     (SQLite), and **deterministic simulation testing** (FoundationDB), for which DB25 is
     unusually well-positioned because it is already deterministic/single-threaded/arena-
     based/exception-free.
  2. **A vectorized, push-based, morsel-driven executor** as the next-layer model
     (DuckDB and Polars independently converged on it).
  3. **Generalizing decorrelation** and adding cost-based passes (join reordering +
     statistics) later.
- **Formal methods (TLA+/Alloy):** none exist today, and that is currently correct; their
  payoff arrives with concurrency/isolation/recovery at the storage layer. See the
  companion [`formal-methods-proposal.md`](formal-methods-proposal.md).

---

## 1. Formal specifications (TLA+ / Alloy) — audit

**Finding: none found.** No TLA+ (`.tla`/PlusCal), Alloy (`.als`), or TLC config (`.cfg`)
artifacts exist anywhere in DB25 — neither locally (`db25-logical-plan` + the
`parser`/`analyzer`/`tokenizer` submodules) nor across the `space-rf-org` GitHub org
(`search_code` for `extension:tla`, `extension:als`, `TLA+`, `Alloy`, `filename:MODULE`
all returned zero). The project verifies its logical layer with adversarial review +
differential testing; there is no formal-methods tooling in the build (CMake/CTest only).

**Assessment.** Formal methods earn their keep on non-deterministic, stateful, failure-
prone subsystems (MVCC, isolation, WAL/recovery, distributed commit) — the canonical
industrial wins (AWS DynamoDB/S3, MongoDB, CockroachDB, FoundationDB). DB25 has **none of
that surface area yet**: it is a deterministic, single-threaded, read-only query compiler.
Introducing TLA+/Alloy broadly now would largely re-prove what differential testing
already covers.

**Recommendation (summary; full proposal in the companion doc):** do **not** adopt a
standing formal-methods methodology for the logical layer. Optionally run one **Alloy**
model of *subquery-decorrelation equivalence* under bag + three-valued (NULL) semantics
(the riskiest rewrite) as a design-verification spike, and treat the positional-column
remap invariant as a **C++ property test** first. **Bank TLA+ for the storage/transaction
layer**, and when it arrives, spec the isolation and recovery protocols *before*
implementing them.

---

## 2. DB25 vs. FoundationDB

### What each is

| | **FoundationDB** | **DB25** |
|---|---|---|
| Category | An ordered, transactional, **distributed key-value store** — *not* a SQL database. SQL/documents/records are optional **layers** on top of the KV core. | A from-scratch **SQL front end**; no execution or storage yet. |
| Core guarantee | Strict serializability over lexicographically-ordered keys. | Deterministic correctness of *query rewrites*. |
| Architecture | "Unbundled": separate **coordination**, **transaction** (Sequencer/Proxies/Resolvers), **log** (WAL), and **storage** subsystems; transaction processes stateless; log-structured storage. | One repo per pipeline stage; owned typed `Expr` IR; rewrites gated by review + differential tests. |
| Concurrency | MVCC + **optimistic** concurrency; conflicts resolved at commit. | N/A today. |
| Testing culture | **Deterministic simulation testing** via **Flow** (a C++ actor framework whose whole cluster runs in one deterministic thread); mocked IO/clock/network, injected faults, seeded `BUGGIFY` points; ~1 trillion CPU-hours simulated. | Adversarial review + differential tests. |

### Scope-mismatch caveat

Most of what makes FDB *FDB* — transactions, MVCC, optimistic conflict resolution, log-
structured storage, distribution, coordination — solves problems DB25 does **not** have
(single-node, single-threaded, read-only, no durable state). The transferable material is
about **engineering method now**, and **storage/transaction design much later**. Don't
force analogies (e.g. "resolvers ≈ binder" is noise).

### Inspiration to take (ranked)

1. **Deterministic simulation testing (Flow/`BUGGIFY`) — HIGHEST value, start NOW.**
   DB25 is *already* what Flow works hard to engineer FDB into: deterministic, single-
   threaded, arena-based, exception-free. Determinism + fault injection turns rare bugs
   into reproducible-from-a-seed bugs.
   - *Now:* a **seeded query fuzzer** that generates random-but-valid SQL/AST, runs the
     full tokenizer→optimizer pipeline, and asserts invariants (IR well-formedness,
     positional-ref integrity after pruning, type consistency, pass idempotence, and —
     the payoff — **semantic equivalence of pre- vs. post-rewrite plans** via differential
     evaluation). Strict superset of the current per-rewrite tests: it finds *interactions
     between* passes (fold × pushdown × decorrelation).
   - Borrow **`BUGGIFY`** directly: seed-controlled injection points that force legal-but-
     unusual optimizer choices (disable a rewrite, flip a boolean-simplification branch,
     perturb/exhaust the arena, reorder independent predicates); any semantic change is a
     bug.
   - *Later (executor/storage):* run the whole engine under a single-threaded simulator
     with mocked clock/IO and injected disk/OOM/short-read faults **before** going multi-
     threaded or durable. It doesn't replace adversarial review — it industrializes it.

2. **The "layer" philosophy — thin, strongly-specified core; features as stateless layers
   — MEDIUM-HIGH, apply as a design discipline.** FDB's founding question was "what can we
   *take away*." Two lessons: (a) **the contract between stages (the typed `Expr` IR) is
   the product** — specify its invariants/ownership/positional-ref semantics as rigorously
   as FDB specs its KV API, so any stage is a replaceable layer over a frozen interface;
   (b) **statelessness** — keep passes pure functions over IR (state in the arena, threaded
   explicitly), which is what later enables trivial parallelization and simulation.

3. **The Record Layer's structured-records-on-KV mapping — MEDIUM, only WHEN storage
   exists.** Bank two ideas for a future storage engine: the **order-preserving composite
   key encoding** (turns range scans into sorted access — the basis of index-driven
   physical operators), and the **capability-driven planner** discipline (emit a sort-free
   or merge-join plan only when a physical access path actually provides the required
   order).

4. **MVCC + optimistic concurrency — LOW now, revisit ONLY at the transactional-storage
   layer.** FDB's separation of *version assignment* (Sequencer) from *conflict detection*
   (Resolvers) from *durability* (LogServers) is a proven template to study *then*.

### Skip

The distributed/coordination machinery (Coordinators, ClusterController, proxies, quorum
recovery, sharding); building a full Flow-style actor/async runtime (you want the
*simulator + `BUGGIFY` methodology*, not the async programming model); log-structured
storage/WAL concerns until a storage engine exists.

*Sources: FoundationDB SIGMOD 2021 paper; apple.github.io/foundationdb architecture,
layer-concept, testing, and Flow docs; Record Layer paper (arXiv 1901.04452).*

---

## 3. DB25 vs. DuckDB and Polars

The two most instructive mirrors — both in-process analytical engines pairing a rewrite-
heavy optimizer with vectorized, columnar execution (the two layers DB25 hasn't built).

### Architectural comparison

| Dimension | DB25 | DuckDB | Polars |
|---|---|---|---|
| Language | C++23, `-fno-exceptions`, single-threaded, deterministic | C++ | Rust |
| Expression IR | Owned typed `Expr`, **positional** refs, baked type + nullability | `Expression` tree, bound `(table_idx, col_idx)` bindings | `AExpr` interned in an **arena**, referenced by node id |
| Logical/physical boundary | **Not yet crossed** | Explicit: `Optimizer` emits logical; `PhysicalPlanGenerator` picks a physical op per node | Explicit: `lower_ir()` → `PhysNodeKind` for the streaming engine |
| Optimizer style | Rule-based, deterministic, each differentially tested | **Rule + cost-based**; join reordering via **DPhyp** over hypergraphs + statistics | Rule-based over the IR arena, gated by `OptFlags` |
| Execution | **None yet** | Vectorized (~2048-row vectors), **push-based**, **morsel-driven parallelism** | Streaming engine (`polars-stream`): morsel-driven, hybrid push/pull, spillable sinks |
| In-memory format | Owned; none columnar yet | Own columnar vectors | **Apache Arrow** |

### Optimizer passes DB25 has vs. is missing

**Has:** constant folding, boolean simplification, predicate pushdown, column pruning,
subquery decorrelation — maps onto the front half of both engines' pipelines.

**Missing (≈ in order of eventual value):**
- **Cost-based join reordering** (DuckDB's DPhyp over a join hypergraph + statistics) —
  the single biggest gap; impossible without statistics/cardinality estimation, which
  DB25 also lacks.
- **Statistics propagation / cardinality estimation** — DB25 has no cost notion; rewrites
  are purely structural.
- **Projection pushdown *into scans*** (distinct from column pruning) — Polars pushes the
  projection into the reader so only needed columns materialize. Pays off once a
  scan/storage layer exists.
- **Predicate pushdown *into scans* / to storage** — row-group/zone-map skipping, late
  materialization. Gated on storage.
- **Common-subexpression *and* common-subplan elimination** — Polars does both.
- **Slice/limit pushdown** — cheap, worth stealing.

### Decorrelation: subset vs. general

DB25 decorrelates **shape-by-shape** (EXISTS/IN/correlated-scalar → semi/anti/left joins;
correlated-aggregate-scalar → LEFT JOIN + GROUP BY), each shape adversarially reviewed.
DuckDB implements the **general** algorithm from Neumann & Kemper, *"Unnesting Arbitrary
Queries"* (BTW 2015): a `LogicalDependentJoin` + a `FlattenDependentJoins` pass that pushes
the dependent join through the correlated subtree, using **DELIM joins** / **`DELIM_GET`**
(replays the distinct set of correlation values) and NULL-safe `COMPARE_NOT_DISTINCT_FROM`
join-back. DuckDB *always* decorrelates *every* subquery — no "unsupported shape" fallback
— and is moving to a top-down strategy (Neumann, BTW 2025).

**Verdict:** DB25's shape-restricted approach is a **reasonable subset, not a dead-end** —
the shapes it handles are the common cases, and its hoist-into-join-condition rewrite is a
hand-specialized instance of what the general algorithm produces. The dead-end risk is at
the edges (arbitrary correlation depth; correlation under LIMIT/window/DISTINCT/set-ops;
non-equi correlation) — each of which would otherwise need a *new* hand-verified shape.
The general frame subsumes all of them under one operator with one correctness argument:
per-shape verification scales linearly with shapes, the general algorithm is O(1) shapes.

### Inspiration to take (ranked, with WHEN)

1. **Adopt the dependent-join / general decorrelation frame — logical optimizer, next.**
   Refactor the shape rewrites to emit a single internal *dependent-join* operator, then
   flatten it Neumann–Kemper-style (push-down + a `DELIM_GET`-equivalent distinct-
   correlation source + NULL-safe join-back). Keep the current per-shape differential
   tests as the oracle/regression coverage. Squarely in DB25's current layer; plays to its
   verification strength; converts linear-in-shapes maintenance into O(1).
2. **Vectorized, push-based execution + morsel-driven parallelism — the executor (layer
   after next).** Both engines converged on the TUM morsel-driven model. Design the
   executor push-based (stateless operator sinks) with fixed-size vectors from day one —
   retrofitting vectorization onto a tuple-at-a-time executor is the classic rewrite trap.
   Morsel parallelism can come later, but the push-based/stateless *interface* must be
   decided up front even while single-threaded.
3. **Draw the logical/physical boundary explicitly, thread a physical-property model — the
   physical planner (immediately next).** Copy DuckDB's split: optimizer output stays
   logical (*what*); a distinct planner picks physical ops from shape/predicates/
   orderedness. DB25's positional Expr IR is ideal input — physical ops index by slot, no
   name resolution. Keep the logical plan immutable across the boundary.
4. **Projection/predicate pushdown-into-scan + slice pushdown — logical optimizer, once a
   scan/storage node exists.** Cheap, deterministic, rule-based (DB25's style); payoff is a
   function of a reader that honors column/row/zone-map skipping.
5. **Statistics + cost-based join reordering (DPhyp) — logical optimizer, last.** Highest
   ceiling, deepest prerequisite chain (stats → cardinality → cost model → DPhyp), and it
   breaks DB25's "purely structural, deterministic" property. Don't start before the
   executor/storage can produce statistics.
6. **Common-subplan / common-subexpression elimination — opportunistic.** Low-risk over
   the typed Expr IR; pairs naturally with a future CTE/materialization story.
7. **Apache Arrow — evaluate at the executor; lean "interop-only, not internal."** Mirror
   DuckDB: own your internal vector format, expose Arrow as import/export. Arrow's layout
   constrains operator design and isn't obviously optimal for a `-fno-exceptions`
   deterministic C++ engine.

### DB25 strengths to keep

The owned typed Expr IR with **positional** column references and baked type/nullability is
ahead of the curve — the name-free, slot-indexed representation both engines converge
toward — and will make the physical planner and executor dramatically simpler. Preserve the
deterministic/single-threaded/exception-free discipline and per-rewrite adversarial-review-
plus-differential-testing as a correctness moat; when cost-based (non-deterministic) passes
arrive, keep the structural passes deterministic and quarantine data-dependent decisions
behind the statistics layer.

*Sources: DuckDB & Polars DeepWiki architecture/optimizer pages; duckdb.org (why_duckdb,
2023 correlated-subqueries blog); Neumann–Kemper BTW2015; DuckDB flatten_dependent_join.cpp
& PR #17294; Polars "in Aggregate Dec'25".*

---

## 4. DB25 vs. SQLite

SQLite and DB25 sit at nearly opposite corners: SQLite is a *complete, shipping* engine
optimized for OLTP/row-store — the case DB25 is *not* targeting — yet it is the most
rigorously engineered SQL implementation in existence, and several lessons transfer
regardless of workload.

### What SQLite is

- **Front end:** tokenizer → parser (Lemon) → **code generator** that walks the parse tree
  and **emits VDBE bytecode more or less directly** — no rich long-lived logical plan
  lowered through a physical planner.
- **Back end:** the **VDBE** bytecode interpreter, the **B-tree** layer, the **pager**
  (cache + transactions + WAL/rollback journal), and the **OS interface (VFS)**.
- **Execution model:** SQL is compiled to a linear array of `VdbeOp` structs; a **register-
  based virtual machine** (~192 opcodes) executes it **row-at-a-time** (`OpenRead`,
  `Rewind`, `Column`, `SeekGE`, `Next`, `ResultRow` walk a B-tree cursor one row per
  iteration). A *completely different* paradigm from DuckDB/Polars vectorized execution —
  trading throughput for tiny code size, portability, and easy correctness.
- **Planner (NGQP):** genuinely cost-based but *modest/pragmatic* — index & join-order
  selection via the **"N Nearest Neighbors" (N3)** beam search (not exhaustive DP),
  `ANALYZE`-driven selectivity (`sqlite_stat1`/`stat4`), and tree-level logical rewrites:
  **subquery flattening** (FROM-clause merge, gated by a long correctness checklist),
  WHERE push-down into subqueries/views, ORDER-BY-via-index, the IN-as-index optimization,
  co-routine evaluation of FROM subqueries.
- **Correlated subqueries:** generally evaluated by **repeated (nested-loop) execution per
  outer row**, often via a co-routine — **not** decorrelated into semi/anti/left join
  operators.
- **Scope:** embedded, serverless, single-file, **row-oriented OLTP** on a B-tree. Famous
  for ubiquity and reliability, not analytical performance.

### The legendary testing (the most transferable lesson)

- **~590× as much test code as core code** (~92M lines of tests/harness vs ~156 KLOC core).
- **100% branch coverage AND 100% MC/DC** (the DO-178B avionics standard) over the core,
  via the **cov1** subset of the proprietary **TH3** harness, verified with `gcov`.
- **Four independent harnesses** (TCL, TH3, SLT, …) so no single framework's blind spots
  dominate.
- **Aggressive fuzzing:** **dbsqlfuzz** (libFuzzer; mutates *SQL and the database file
  simultaneously*) at ~1 billion mutations/day; historical crashers frozen and replayed by
  `fuzzcheck` on every `make test`.
- **Fault injection:** **OOM/`malloc`-failure injection** on every allocation site,
  **I/O-error injection**, and **crash/power-loss testing** for pager ACID recovery.
- A documented **tension**: MC/DC-perfect code tends to be *more* fuzz-vulnerable and
  vice-versa, so SQLite runs both because they catch disjoint bug classes.

### Head-to-head

| Dimension | SQLite | DB25 |
|---|---|---|
| Execution | Register-based **VDBE bytecode VM**, row-at-a-time; shipping | **None yet** |
| Planner | Cost-based **NGQP**, N3 beam search, stat-driven; pragmatic | Logical optimizer only (no cost model) |
| IR | Bytecode emitted ~directly from parse tree; **thin logical IR** | **Richer owned typed Expr IR**, positional refs |
| Subqueries | Flattening + WHERE push-down; correlated → repeated/co-routine eval | **Full decorrelation** → semi/anti/left joins |
| Testing | 590× ratio, **100% MC/DC**, 4 harnesses, ~1B fuzz/day, OOM/IO/crash injection | Adversarial review + differential tests |
| Scope | Complete OLTP row-store engine | Analytical-leaning front end |

**Is DB25 "ahead" on IR?** For its target, yes — meaningfully. SQLite intentionally keeps
almost no durable logical-plan IR (it lowers straight to bytecode); the right call for a
tiny embedded row engine, but it forecloses exactly the analytical rewrites DB25 already
performs. DB25 is ahead on the *representation* while behind on being an actual engine.

### Inspiration to take (ranked, with WHEN)

1. **Testing rigor — adopt incrementally, starting NOW (highest priority).**
   - *Now (logical layer):* keep adversarial review + differential tests, but add
     **coverage measurement** (`gcov`/`llvm-cov`) and drive toward **100% branch coverage**
     of the optimizer/binder, then **MC/DC on the rewrite predicates** (decorrelation /
     pushdown legality conditions are exactly the compound boolean guards MC/DC targets).
     Add **grammar-based SQL fuzzing** asserting plan-validity + type/nullability
     preservation (cheap given the deterministic, exception-free design).
   - *Later (executor/storage):* **OOM injection** becomes essential for `-fno-exceptions`
     (every arena/allocation-failure path must be tested — no stack unwinding to rely on),
     plus **I/O-error** and eventually **crash/power-loss** testing. Freeze every fuzzer
     crasher and replay forever. Run both MC/DC and fuzzing (the disjoint-bug-class
     tension).
2. **Execution-model decision — VDBE is a good *correctness-first starting point* but the
   *wrong long-term target* for an analytical engine.** A bytecode VM is easy to make
   correct, test, debug (single-step / EXPLAIN), and deterministic — but it's row-at-a-
   time. *When each is right:* bytecode/tuple-at-a-time VM when correctness/small-code/
   portability/point-lookups dominate (SQLite); **vectorized pull/push when scanning &
   aggregating large batches dominate (DB25)**. *Recommended path:* a simple tuple-/block-
   at-a-time (Volcano-style) interpreter is a legitimate v1 for correctness — but design
   the operator interface and Expr IR so the hot path can become **vectorized batch
   evaluation** rather than committing to a scalar register VM you'd throw away. Borrow
   VDBE's *testability and determinism*, not its row-at-a-time core.
3. **Pragmatic, cost-modest planning — WHEN you add the physical planner.** SQLite shows
   most of the win comes from a **bounded-search** cost model (N3 beam search over join
   order + simple stat-driven selectivity). Resist a full Cascades/Selinger build-out for
   v1; a bounded beam search + `stat4`-style histograms gets ~80% at a fraction of the
   complexity.
4. **Portability / zero-dependency / amalgamation ethos — WHEN packaging.** DB25's per-
   stage, C++23, `-fno-exceptions`, dependency-light design already shares this spirit;
   keep the whole pipeline buildable as a small self-contained unit.

### Flattener vs. decorrelation

Different problems. SQLite's **flattener** is a **FROM-clause merge** — it lifts a
derived-table/view subquery's FROM+WHERE into the outer query to avoid materializing an
unindexed transient, guarded by a syntactic legality checklist. It does **not** turn a
correlated *predicate* subquery into a join operator — SQLite still evaluates those by
repeated per-row / co-routine execution. DB25's decorrelation is strictly more powerful:
it rewrites correlated EXISTS/IN/correlated-scalar into **semi/anti/left join operators** a
set-oriented (hash/merge) executor runs *once* instead of N times. Complementary: DB25
should *also* adopt SQLite-style FROM-subquery flattening / WHERE push-down (cheap, high-
value, feeds pushdown/pruning), but its decorrelation already reaches semantics SQLite's
planner deliberately never attempts.

### DB25 strengths to keep

The richer owned typed Expr IR with positional refs + baked type/nullability, the explicit
logical plan enabling real algebraic rewrites, and — above all — decorrelation into
semi/anti/left joins (genuinely ahead of SQLite for an analytical target). Pair these with
SQLite-grade coverage + fuzzing + fault-injection discipline as the executor and storage
layers land.

*Sources: sqlite.org (arch, opcode, vdbe, queryplanner-ng, optoverview, testing);
sqlite.org forum on correlation; ascii.co.uk on the test ratio.*

---

## 5. Synthesis — a suggested sequence for the next layers

1. **Now (logical layer):**
   - Invest in **testing infrastructure**: coverage → MC/DC on rewrite predicates;
     grammar-based **SQL fuzzing** with a **deterministic simulation** harness (FDB-style
     seeds + `BUGGIFY` injection) checking **pre/post-rewrite semantic equivalence** and IR
     invariants. This is the highest-ROI investment available today and plays to DB25's
     deterministic design.
   - Optionally begin the **general decorrelation (dependent-join) refactor**, with the
     existing per-shape tests + an optional Alloy equivalence model as the oracle.
   - Adopt cheap wins: **FROM-subquery flattening**, **slice/limit pushdown**, **CSE**.
2. **Physical planner (next layer):** draw an **explicit logical/physical boundary** (keep
   the logical plan immutable); pick physical operators from shape/predicates/orderedness;
   thread a **physical-property** model (orderedness/partitioning). Start cost-modest
   (bounded beam search) rather than full DP.
3. **Executor:** **push-based, vectorized, stateless-operator** interface from day one
   (columnar batches); morsel-driven parallelism later; Arrow as interop only. A Volcano
   interpreter is an acceptable correctness-first v1 *if* the interface is vectorization-
   ready. Add **OOM/IO fault injection** here.
4. **Statistics + cost-based join reordering (DPhyp):** last, once the executor/storage can
   produce base-table statistics; quarantine data-dependent decisions behind the stats
   layer to preserve deterministic structural passes.
5. **Storage + transactions (much later):** adopt **TLA+** as a first-class practice —
   spec the isolation and recovery protocols *before* implementing them (see the companion
   proposal); steal FoundationDB's order-preserving key encoding and its separation of
   version-assignment / conflict-detection / durability.
