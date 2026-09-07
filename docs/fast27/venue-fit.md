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

Deadlines verified 2026-09-07 via search; the conference sites themselves
(`cidrdb.org`, `wikicfp.com`) are egress-blocked from this environment, so
**confirm each against the official CFP before planning around it.**

| Venue | What you'd submit | Next deadline | Fit |
|---|---|---|---|
| **PVLDB Vol. 20** → VLDB 2027 | The optimizer and the falsifiability-gate methodology, with an evaluation. Monthly deadlines make this the most schedule-flexible option in the field. | **Thu Oct 1, 2026** (monthly, 1st of each month; CMT opens the 20th of the prior month). Final Vol. 20 deadline Mar 1, 2027; revisions after Jun 1, 2027 roll to VLDB 2028. | **Best near-term.** ~3 weeks. Needs an evaluation — see below. |
| **EDBT 2027** | Any single stage as a focused contribution, or the architecture paper. | **Wed Oct 7, 2026** (3rd/final cycle). Conference Apr 6–9, 2027, Lille, France. | **Good.** ~4 weeks. Last cycle for the 2027 edition. |
| **SIGMOD 2027** | The architecture + methodology paper at the highest bar. | **Sat Oct 17, 2026** (Round 4; abstract due ~Oct 10). Conference Jun 13–19, 2027, Huntington Beach, CA. | **Good, hardest bar.** ~6 weeks. |
| **DaMoN 2027** (with SIGMOD) | The SIMD tokenizer, the arena allocator, or both as one hardware-conscious frontend paper. Both existing PDFs are ~80% of a submission already. | **Not yet announced.** Historically ~mid-March (DaMoN '25 was Mar 14); workshop runs with SIGMOD in June. Watch <https://damon-db.org/>. | **Excellent fit**, ~6 months out. Fix the numeric problems in `readiness-assessment.md` §4 first. |
| ~~**CIDR 2027**~~ | — | **CLOSED.** Deadline was Aug 4, 2026, 11:59pm PT — it passed ~5 weeks ago. Notifications Oct 6, 2026; conference Jan 24–27, 2027, Amsterdam. | Would have been the best fit. **CIDR 2028** (expect ~Aug 2027) is the next shot. |
| **FAST '28** | Framing A, with the storage work in §6 actually done. | ~Sept 2027 (fall cycle) | **Achievable in a year** — see §6. |

### What changed, and why it's good news

CIDR 2027 — the venue this packet originally recommended — closed on Aug 4, 2026.
That is a real loss: CIDR is built for exactly this kind of "here is a whole
engine designed a particular way" paper, it accepts short papers, it prizes
architecture over incremental results, and it is **single-blind** (authors put
their names on the first page), which would have removed the entire
anonymization problem in `submission-checklist.md` §2. Put CIDR 2028 on the
calendar now.

But the replacement options are all *better than the FAST slot in every respect
except one*: they are 3–6 weeks out instead of 8 days, and they are reviewed by
database people. The FAST deadline is not the last train — it is the wrong train
that happens to be leaving first.

### The catch, stated plainly

PVLDB, EDBT, and SIGMOD are full research tracks. They will not accept an
architecture paper with no evaluation, which CIDR would have. So the extra three
to six weeks are not slack — they are exactly the time needed to build the
evaluation that `readiness-assessment.md` §3 lists as gap #4.

The good news is that a **credible evaluation is possible without an execution
engine**, which is not true for FAST:

- **Plan quality vs. real optimizers.** Run the corpus through PostgreSQL and
  DuckDB, capture their plans, and compare join orders and access-path choices
  against DB25's. Agreement is evidence the cost model is sane; disagreement is a
  finding worth a subsection either way. No executor needed — only plans.
- **Planner latency vs. query complexity**, with the search-budget guard from
  `test_search_budget.cpp`. Already measurable today.
- **Falsifiability-gate results**, quantified: mutants injected, mutants caught,
  vacuous tests found and what they were. This is the most novel number in the
  stack and nobody else reports it.
- **Coverage**: the 344-query corpus and 47 staged fixtures against the SQL
  surface, with the gap register as the honest limitations section.

That is a real evaluation section for a planner paper. It is not a storage
evaluation, which is precisely why these venues work and FAST does not.

### Recommended plan

1. **Target PVLDB Oct 1** if the evaluation above can be built in three weeks;
   otherwise **EDBT Oct 7** or **SIGMOD Round 4 Oct 17**, in that order of
   preference by how much time you actually have. PVLDB's monthly cadence means
   slipping a month costs a month, not a year — start there and slide if needed.
2. **Put CIDR 2028 (~Aug 2027) on the calendar.** It remains the single best fit
   for the architecture paper.
3. **Watch `damon-db.org`** for the 2027 CFP (~March) and fix the tokenizer
   paper's numbers in the meantime.
4. **Drop FAST '27.** Revisit for FAST '28 only if §6 gets done.

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

---

## Sources for §5

Checked 2026-09-07. `cidrdb.org` and `wikicfp.com` are egress-blocked from this
environment, so those rows come from search results rather than the pages
themselves — verify before planning around them.

- [CIDR 2027 CFP](https://www.cidrdb.org/cidr2027/cfp.html) · [CIDR 2027](https://www.cidrdb.org/cidr2027/)
- [PVLDB Vol. 20 submission guidelines](https://www.vldb.org/2027/submission-guidelines.html) · [VLDB 2027 research track](https://vldb.org/2027/call-for-research-track.html)
- [SIGMOD 2027 important dates](https://2027.sigmod.org/calls_papers_important_dates.shtml) · [SIGMOD 2027 research CFP](https://2027.sigmod.org/calls_papers_sigmod_research.shtml)
- [EDBT/ICDT 2027](https://edbticdt2027.github.io/) · [EDBT 2027 CFP listing](https://www.madics.fr/event/conf615/)
- [DaMoN](https://damon-db.org/)
