# The 8-day plan, if you submit anyway

[`venue-fit.md`](venue-fit.md) recommends against submitting the DB25 frontend to
FAST '27. This is the plan if you decide otherwise. It is written as a real plan
— if you're going, go properly.

**Today: 2026-09-07. Deadline: 2026-09-15 23:59 AoE (= 12:00 UTC Sept 16).**

---

## Read this first

The plan below produces a **6-page short paper** under Framing A (storage
substrate as a typed planner input — see `venue-fit.md` §4). Two things to be
clear-eyed about:

- **A 12-page long paper is not achievable.** It needs an evaluation, and there
  is no execution engine to measure. Don't attempt it.
- **Even the short paper is a long shot.** It will be judged by storage
  researchers on a system that has no I/O in its cost model. The realistic
  outcome is a scope rejection with reviews you can use. If reviews-as-feedback
  is worth eight days to you, that's a legitimate reason to go. If you need an
  acceptance, aim at one of the deadlines below instead.

**The genuinely better use of these eight days** is the first eight days of a
three-week run at **PVLDB's Oct 1 deadline** (or EDBT Oct 7 / SIGMOD Round 4
Oct 17). All three are database venues with database reviewers, all are further
out than Sept 15, and `docs/design/physical-planner.md` plus
`docs/gap-register.md` are already most of a draft. The tradeoff is that they are
research tracks and need an evaluation — but one is buildable without an
execution engine (plan comparison against PostgreSQL and DuckDB, planner
latency, falsifiability-gate numbers). See [`venue-fit.md`](venue-fit.md) §5.

CIDR 2027 would have been the ideal home and needed no evaluation at all, but its
deadline passed on Aug 4, 2026. CIDR 2028 (~Aug 2027) is the next shot.

## The claim the short paper would make

> A query planner can treat the storage substrate as a declarative, versioned,
> external input rather than as knowledge baked into planner code — and can be
> built, golden-tested, and shown correct **before any execution engine or
> storage layer exists**. We present the capability-profile contract that makes
> this possible, the determinism invariant it rests on, and the falsifiability
> gate that establishes the resulting tests are capable of failing.

That claim is *true of DB25 today* and needs no new measurement — which is the
only reason a submission is possible at all this week. Its weakness is equally
plain: the storage substrate is currently hypothetical, so the paper argues an
architecture rather than demonstrating a storage result. Say so in the paper
rather than letting a reviewer find it.

## Day by day

**Day 1 (Mon Sept 8) — decide, and set up.**
- Reconsider once more against the PVLDB Oct 1 option — three weeks and a
  database PC beats eight days and a storage PC. *(1h)*
- Commit to long/short. Recommendation: short, 6 pages. *(—)*
- Download the current USENIX template; get a title page and section skeleton
  compiling. Do not reuse the IEEEtran files in these repos. *(1h)*
- Add the storage vocabulary to the capability profile — at minimum define
  `StorageSubstrate` in the spec with two concrete instances (local NVMe,
  disaggregated object store), even if nothing costs them yet. Without this the
  paper has no storage noun to point at. *(4h)*

**Day 2 (Tue Sept 9) — the thing that makes it a storage paper.**
- Add I/O terms to `calibration.lab.sexpr`: page-read, sequential-vs-random,
  device-latency. Wire them into the cost model. *(6h)*
- Show a plan flip: one query where the chosen plan differs between the two
  substrate profiles, with nothing but the profile changed. **This single result
  is the paper's spine.** *(2h)*

**Day 3 (Wed Sept 10) — related work.**
- 30–50 references. Cascades/Volcano, Orca, Calcite, System-R, DuckDB, Umbra,
  Photon; and on the storage side computational storage, pushdown-to-device,
  ZNS, tiering, disaggregation. There are currently **zero** citations anywhere
  in the DB25 docs — this is a full day, not an evening. *(8h)*

**Day 4 (Thu Sept 11) — write the core.**
- Design section from `docs/design/physical-planner.md` — it is already good
  prose and mostly needs compression and de-first-personing. *(4h)*
- Methodology section: the falsifiability gate, staged goldens, round-trip and
  injection. *(3h)*

**Day 5 (Fri Sept 12) — evaluation, such as it is.**
- Plan-flip table across substrate profiles. *(3h)*
- Planner latency: search time vs. query complexity, with the budget guard from
  `test_search_budget.cpp`. Real, measurable, honest. *(3h)*
- Gate results: mutants injected, mutants caught, vacuous tests found. Quantified
  and unusual — lead the evaluation with it. *(2h)*

**Day 6 (Sat Sept 13) — intro, abstract, limitations.**
- Intro and abstract last, as always. Draft in `paper/abstracts.md`. *(4h)*
- Limitations section, written honestly: no execution engine, no measured I/O,
  cost model uncalibrated against a real device. Volunteering this is strictly
  better than having a reviewer discover it. `docs/gap-register.md` is the model
  for the tone. *(2h)*
- Figures: pipeline diagram, memo/search diagram, plan-flip diagram. Greyscale-legible. *(2h)*

**Day 7 (Sun Sept 14) — polish and anonymize.**
- Full anonymization sweep — [`submission-checklist.md`](submission-checklist.md)
  §2, every box. Budget real time; this is where papers die. *(3h)*
- Formatting compliance; page count excluding references. *(1h)*
- Read the whole thing aloud. Fix the arithmetic on every claimed number. *(3h)*
- Have someone read it cold and try to name the authors. *(1h)*

**Day 8 (Mon Sept 15) — submit.**
- HotCRP submission by **midday your time**, not at AoE. *(1h)*
- Download your own submission, read it, re-run `pdfinfo`. *(30m)*
- Tag the submission commit in every repo. *(15m)*
- Remaining hours are float. You will need them.

## Cut lines, in order

Under time pressure, drop in this order:

1. The disaggregated-object-store second profile — one substrate plus a described
   second is survivable.
2. Planner-latency numbers — keep the plan-flip table and the gate results.
3. Figures beyond the pipeline diagram.
4. **Never cut:** the anonymization sweep, the limitations section, or the
   arithmetic check on headline numbers.

## Kill criteria

Stop and redirect to PVLDB if, by **end of Day 2**, the plan flip doesn't
materialize — if adding I/O terms doesn't change any plan, there is no storage
result and the paper has no spine. That's a clean, early, cheap decision point,
and taking it still leaves three full weeks before PVLDB's Oct 1 deadline — and
the I/O cost-model work from Days 1–2 is not wasted, it goes straight into that
paper's design section.
