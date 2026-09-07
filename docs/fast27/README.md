# FAST '27 Conference Readiness Packet

Everything needed to decide on, prepare, and submit a paper to the **25th USENIX
Conference on File and Storage Technologies (FAST '27)**, Feb 23–25 2027, Hyatt
Regency Lake Washington, Renton WA.

**Packet compiled:** 2026-09-07 · **Fall submission deadline:** 2026-09-15, 23:59 AoE

---

## The one-paragraph verdict

**Do not submit the DB25 frontend to the FAST '27 fall deadline.** Two independent
blockers, either of which is sufficient on its own:

1. **Scope.** FAST reviews storage systems. DB25 as it stands today contains no
   storage system, no I/O path, and no I/O term in its cost model — the pinned
   cost profile (`db25-physical-plan/spec/calibration.lab.sexpr`) prices only CPU
   work per row (`scan-row`, `hash-probe-row`, `sort-row`, `simd-width`,
   `cache-line`). `db25-execution-ref` has zero commits. A submission would be
   scored out of scope by a storage PC, not merely rejected on quality.
2. **Time.** Eight days remain. A 12-page FAST paper needs an evaluation section,
   and there is no storage measurement of any kind in any repo to build one from.

This is a venue-fit problem, not a work-quality problem. The DB25 stack is
substantial and unusually well-tested; it is aimed at the wrong conference. See
[`venue-fit.md`](venue-fit.md) for the ranked alternatives — **CIDR 2027 and DaMoN
2027 are strong, near-term, genuinely appropriate targets** for the work that
already exists.

If you disagree and want to go anyway, [`plan-8-day.md`](plan-8-day.md) is the
honest version of that plan, including what it would cost and what it would have
to omit. It is written as a real plan, not a discouragement.

---

## What's in the packet

| File | What it's for |
|---|---|
| [`cfp-facts.md`](cfp-facts.md) | Every hard date, page limit, and formatting rule, with sourcing and a verification warning |
| [`venue-fit.md`](venue-fit.md) | Scope analysis, three candidate FAST framings ranked by honest fit, and alternative venues with deadlines |
| [`readiness-assessment.md`](readiness-assessment.md) | Asset-by-asset inventory of what exists across the nine repos, and the gap list between that and a submittable paper |
| [`submission-checklist.md`](submission-checklist.md) | Mechanical pre-submission checklist — double-blind sweep, formatting, PDF hygiene, HotCRP |
| [`artifact-evaluation.md`](artifact-evaluation.md) | Artifact Evaluation prep. DB25's strongest card; what to do to the repos to earn all three badges |
| [`plan-8-day.md`](plan-8-day.md) | Day-by-day plan if you submit to the Sept 15 deadline anyway |
| [`paper/`](paper/) | USENIX-format anonymized LaTeX skeleton + drafted abstracts for each candidate framing |

## Read in this order

1. `cfp-facts.md` — confirm the dates against the live CFP first; this container
   could not reach usenix.org.
2. `venue-fit.md` — the decision.
3. `readiness-assessment.md` — what you'd be building on either way.
4. Then either `plan-8-day.md` (go) or the alternative-venue table in
   `venue-fit.md` (redirect).

## Standing caveat

`www.usenix.org` is blocked by this environment's egress proxy, so every CFP fact
in this packet was reconstructed from search results on 2026-09-07 rather than
read off the official page. The dates are internally consistent and cross-checked
against two independent listings, but **verify them against
<https://www.usenix.org/conference/fast27/call-for-papers> before acting.** Any
figure that matters is flagged in `cfp-facts.md` with its confidence.
