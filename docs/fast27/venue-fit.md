# Venue fit: DB25 → FAST '27

The question this document answers: *is there a FAST '27 paper in the DB25 work,
and if not, where does this work belong?*

---

## 1. What FAST reviews

FAST's scope sentence is broad on its face — "low-level storage devices,
distributed storage systems, information and data management, as well as other
systems interconnected with storage." But scope is enforced by the PC, and the
FAST PC is a storage-systems community: flash/ZNS/SMR device behavior, file
system design and crash consistency, key-value stores and LSM engineering,
distributed and disaggregated storage, caching and tiering, dedup and
compression, storage reliability and failure analysis, persistent memory,
computational storage.

The operative test for a FAST paper is: **does the contribution turn on a
property of storage?** Not "does the system eventually touch a disk" — does the
paper's insight, design, and evaluation live in the storage layer.

## 2. What DB25 is

DB25 is a from-scratch SQL **query compiler frontend**, built one repo per stage:

```
tokenizer → parser → semantic analyzer → binder → logical optimizer → physical planner
```

Everything downstream of the physical planner is unbuilt. Specifically:

- `db25-execution-ref` — **zero commits.** There is no execution engine.
- `db25-physical-plan` — at Increment 5.1. It *emits* plans and explicitly "never
  runs one" (`db25-physical-plan/README.md`). Its design doc states the planner
  is "the last stage that is still pure metadata."
- There is no buffer pool, no page layout, no file format, no write path, no
  durability mechanism, no I/O scheduler, no storage device model anywhere in the
  nine repos.

The decisive artifact is the pinned cost model,
`db25-physical-plan/spec/calibration.lab.sexpr`. Every coefficient in it is
CPU-per-row:

```
(scan-row 1.0) (column-scan-row 0.35) (filter-row 0.5) (project-row 0.3)
(hash-build-row 1.2) (hash-probe-row 0.8) (merge-join-row 0.4)
(nested-loop-pair 0.30) (sort-row 1.0) (convert-row 0.6)
(simd-width 8) (cache-line 64)
```

**There is no I/O term.** No page-read cost, no device latency, no
sequential-vs-random asymmetry, no cache-miss model against a storage hierarchy.
The two "hardware" parameters are SIMD width and cache line — both CPU. A cost
model with no I/O term is a precise, checkable statement that this system does not
currently reason about storage at all.

## 3. Verdict

**There is no FAST '27 paper in DB25 as it stands.** This is not a close call and
not a matter of framing. A storage PC reading a SQL-frontend paper with a
CPU-only cost model and no execution engine will score it out of scope in the
first review round.

Submitting anyway costs more than the eight days: FAST '27 has no later cycle, a
desk-rejected or out-of-scope submission burns the slot, and the same work
submitted to a fitting venue three months later would be reviewed by people
equipped to judge it.

## 4. The candidate framings, ranked honestly

For completeness, here are the three angles anyone would consider, and why each
fails *today*. Framing A is the only one with a genuine future at FAST.

### A. "Planning for the storage substrate before the engine exists" — the only real candidate

**The idea.** DB25's physical planner takes an `ExecutionCapabilityProfile` — a
structural, versioned, external spec describing what the execution engine can do
and what the hardware *is* (operators, encodings, pushdown surface, parallelism
and memory model, SIMD width, NUMA topology) — as a *typed input* to a pure
deterministic function. Nothing about execution leaks in except through that
input. That is a genuinely interesting architectural claim, and its natural
extension is a storage one: **model the storage substrate (NVMe with computational
pushdown, local SSD, disaggregated object store, PMEM tier) as a first-class,
declaratively specified planner input, and show that plan quality tracks the
substrate description without any planner code change.**

Computational storage, pushdown-to-device, and tiering are all live FAST topics.
This framing points at them.

**Why it isn't submittable now.** The claim is empirical — "plan quality tracks
the substrate" — and testing it needs (a) storage terms in the cost model, (b) at
least two real substrates, and (c) an executor to measure realized time against
predicted cost. DB25 has none of the three. Without them the paper is a design
document, and FAST does not publish design documents.

**What would make it real:** see §6.

### B. "Falsifiability-gated testing for systems software"

**The idea.** DB25's test methodology is genuinely unusual and genuinely good: a
mutation-based **falsifiability gate** where the suite must pass clean, every
injected mutant must be caught by ≥1 test, and *any test that no mutant can make
fail is reported as a defect* — a vacuous test. Combined with per-stage golden
s-expressions that localize a regression to the first diverging stage, lossless
round-trip encoding, and stage-isolated injection. All of it runs in `ctest` under
ASan+UBSan in CI.

**Why it isn't a FAST paper.** The methodology is demonstrated on a query
compiler, which is a pure function — the easiest possible target. The reason
storage-systems testing is hard is crash consistency, partial writes, reordering,
fault injection across a durability boundary, and recovery — none of which a pure
function has. A FAST reviewer would ask "does this survive contact with a write
path?" and the answer is: untested. This is a strong paper for a
software-engineering or systems-testing venue, not for FAST.

### C. The existing SIMD tokenizer / arena allocator papers

**Not FAST work.** SIMD lexing and bump-pointer AST allocation are CPU and memory
contributions. No storage content, and no reframing gets them there. They belong
at DaMoN or a similar hardware-conscious data-management workshop — where they'd
be well received. See the note in
[`readiness-assessment.md`](readiness-assessment.md) §4 about two numeric
inconsistencies in the tokenizer paper that must be fixed before it goes anywhere.

## 5. Where this work actually belongs

Ranked by fit against what exists **today**. Confirm all deadlines — they move
year to year and this environment could not reach the conference sites.

| Venue | What you'd submit | Typical deadline | Fit |
|---|---|---|---|
| **CIDR 2027** | The DB25 architecture paper: spec-driven Cascades, the pure-function planner contract, capability profiles, the falsifiability gate. CIDR explicitly wants visionary systems architecture over incremental results, and short papers. | Typically ~Sept–Oct for a January conference — **check immediately, this may be the nearest live deadline** | **Excellent.** CIDR is built for exactly this kind of "here is a whole engine designed a particular way" paper. |
| **DaMoN 2027** (with SIGMOD) | The SIMD tokenizer, the arena allocator, or both as one hardware-conscious frontend paper. Both existing PDFs are ~80% of a DaMoN submission already. | Typically ~March, workshop in June | **Excellent.** DaMoN is the home venue for SIMD-in-a-database-frontend work. |
| **VLDB 2027** | The optimizer + falsifiability-gate methodology, with an evaluation. Rolling monthly deadlines make it schedule-friendly. | Rolling, monthly | **Good**, once there's an evaluation. |
| **SIGMOD 2028** | Same, at a higher bar. | Typically ~Oct / ~Apr rounds | Good, once there's an evaluation. |
| **EDBT 2027** | Any single stage as a focused contribution. | Typically ~Oct | Good. |
| **FAST '28** | Framing A, with the storage work in §6 actually done. | ~Sept 2027 (fall cycle) | **Achievable in a year** — see §6. |

**Immediate action:** check the CIDR 2027 deadline today. If it is still open, the
eight days currently pointed at FAST are far better spent there, and the material
in `docs/design/physical-planner.md` plus `docs/gap-register.md` is already most of
the paper.

## 6. What would make a real FAST '28 submission

If FAST is a goal rather than a coincidence, this is the concrete path. It is
roughly a year of work and all of it is on the critical path for DB25 anyway —
none of it is paper-only busywork.

1. **Land `db25-execution-ref`.** It currently has zero commits. Without a
   running engine there is no measurement and therefore no FAST paper, ever.
2. **Add I/O to the cost model.** Extend `calibration.lab.sexpr` with page-read,
   sequential-vs-random, and device-latency terms. This is the single change that
   converts DB25 from a CPU system into one that reasons about storage.
3. **Make `ExecutionCapabilityProfile` describe storage substrates.** Encode at
   minimum: local NVMe, a disaggregated object store, and one device with a
   pushdown surface. Keep them as external versioned specs — that *is* the
   contribution.
4. **Build the calibration loop.** `CalibrationSource` already exists as a seam
   with a `PinnedLab` implementation. Add a live-measuring source that derives
   coefficients from a real device. "The planner calibrates itself to the storage
   it is given" is a FAST-shaped claim.
5. **Evaluate across substrates.** Same queries, same planner binary, different
   substrate specs; show the plan changes and that predicted cost tracks realized
   time. Report where the model is wrong — FAST reviewers reward honest error
   analysis over clean wins.
6. **Extend the falsifiability gate across the durability boundary.** Mutate the
   write path, inject faults, and show the gate catches what crash-consistency
   test suites miss. This turns Framing B from a compiler result into a storage
   result, and pairs with the rest as a methodology section rather than a
   standalone paper.

Items 1–3 are the gate. With 1–5 done, Framing A is a credible FAST '28 long
paper with a real evaluation; add 6 and it is a strong one.
