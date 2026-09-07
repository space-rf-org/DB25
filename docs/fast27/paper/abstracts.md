# Drafted abstracts

Starting points for each candidate framing, so the abstract isn't written at 3am
on Sept 15. All are **anonymized** and written in third person about the system.

Every `<N>` is a placeholder for a number you must actually measure. Do not ship
an abstract with a number you haven't produced — and do the arithmetic on the
ones you have (see [`../readiness-assessment.md`](../readiness-assessment.md) §4,
where exactly this bit the tokenizer paper).

---

## A. Storage substrate as a typed planner input — the FAST framing

> Query planners bake knowledge of the storage substrate into planner code: the
> cost of a scan, the value of an index, and the profitability of pushdown are
> expressed as branches and constants that must be edited whenever the substrate
> changes. As storage diversifies — local NVMe, disaggregated object stores,
> devices with a computational pushdown surface — this coupling makes the
> planner the least portable component of the engine, and it forces the planner
> to be co-developed with a running execution engine.
>
> We present a physical planner that treats the storage substrate as a
> declarative, versioned, external specification consumed as a typed input to a
> pure function: `plan(logical_plan, catalog_stats, capability_profile,
> runtime_profile?)`. Nothing about execution or storage reaches the planner
> except through those inputs. The design rests on one invariant — a single
> physical IR, a single cost model, and deterministic path selection — which
> makes the planner golden-testable end to end *before any execution engine
> exists*.
>
> Across `<N>` substrate specifications differing only in their declared storage
> characteristics, the same planner binary produces materially different plans
> for `<N>` of `<N>` corpus queries, with no planner code change. We further
> report a falsifiability gate over the planner's test suite: every injected
> mutant is caught by at least one test, and `<N>` tests were found to be
> incapable of failing under any mutation and were repaired. Planning latency
> stays within a `<N>`ms budget at `<N>` relations.

**Honest read:** the strongest FAST-shaped claim available, and it still argues
an architecture more than it demonstrates a storage result. Needs the Day 1–2
work in [`../plan-8-day.md`](../plan-8-day.md) before a single number in it is
real.

---

## B. The architecture paper — the database-venue framing

> Query engines are typically built as a monolith and tested end to end, which
> means the correctness of an early stage is only ever observed through the
> behavior of the last. We describe an engine built as one repository per stage
> — tokenizer, parser, semantic analyzer, binder, logical optimizer, physical
> planner — where every inter-stage artifact has a canonical, losslessly
> round-trippable s-expression encoding, and each stage's output is pinned
> against a committed golden. A regression localizes to the first stage whose
> artifact diverges, rather than to the query that failed.
>
> Two design commitments make this work. The physical planner is a pure,
> deterministic function of `(logical_plan, catalog_stats, capability_profile,
> runtime_profile?)`, so it can be fully tested before an execution engine
> exists; and the optimizer is spec-interpreted rather than hard-coded, so
> adding an operator is a specification edit and a cost function, never a
> planner rewrite.
>
> We report on `<N>`k lines of C++23 across six stages, `<N>` staged golden
> fixtures, and a falsifiability gate that requires every injected mutant to be
> caught by at least one test and reports any test that no mutant can make fail
> as a defect. Applying the gate found `<N>` vacuous tests and `<N>` latent
> defects. We also present a gap register: an inventory in which every known
> frontend gap is either closed or provably incapable of producing a wrong
> result, making phase exit a checklist rather than a judgement call.

**Honest read:** this one is close to true *today* — it was written for CIDR,
which closed Aug 4, 2026. For PVLDB (Oct 1), EDBT (Oct 7), or SIGMOD Round 4
(Oct 17) it needs an evaluation paragraph on top: plan-quality comparison against
PostgreSQL and DuckDB plans, planner latency against the search budget, and the
gate numbers. Note that all three are double-blind, so keep the third-person
phrasing above; CIDR would have been single-blind.

---

## C. Falsifiability gating — the methodology framing

> Systems papers routinely claim comprehensive test suites; almost none present
> evidence that their tests are capable of failing. We describe a falsifiability
> gate applied to a SQL engine's frontend: the suite must pass clean, every
> injected mutant must be caught by at least one test, and any test that no
> mutant can make fail is reported as a defect. A test that cannot fail is
> treated not as harmless but as a bug.
>
> We combine the gate with per-stage golden artifacts that localize a regression
> to the first diverging stage, a lossless round-trip property on the artifact
> encoding, stage-isolated plan injection, and a reference evaluator with
> three-valued logic that verifies rewrites by result equivalence rather than by
> plan shape. Applied across `<N>`k lines and `<N>` stages, the gate identified
> `<N>` vacuous tests and `<N>` defects that the passing suite had not detected.

**Honest read:** genuinely interesting, but demonstrated on a pure function.
Reviewers at a storage venue will ask whether it survives a write path and a
crash-consistency boundary; today the answer is untested. Best as a *section* of
paper A or B rather than standalone — until the gate is extended across a
durability boundary (`../venue-fit.md` §6, item 6).

---

## Checklist for whichever you use

- [ ] Every number measured, not estimated.
- [ ] Divide your own headline numbers by each other and check they're consistent.
- [ ] No first-person reference to prior work ("our earlier system").
- [ ] No system name that trivially resolves to a public repo with your name in it.
- [ ] 150–250 words.
- [ ] Read aloud once. It's the only part of the paper every reviewer reads.
