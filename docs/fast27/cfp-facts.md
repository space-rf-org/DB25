# FAST '27 — Call for Papers, hard facts

**Compiled 2026-09-07.** `www.usenix.org` is blocked by this environment's egress
proxy, so nothing below was read off the official page. Each fact carries a
confidence marker:

- **[2x]** — corroborated by two independent search results
- **[1x]** — single source
- **[inf]** — inferred from USENIX's standing practice, not from a FAST '27 source

**Verify everything marked [1x] or [inf] against the live CFP before relying on
it:** <https://www.usenix.org/conference/fast27/call-for-papers>
(PDF: `usenix.org/sites/default/files/fast27_cfp_031126.pdf`).

---

## Conference

| | |
|---|---|
| Event | 25th USENIX Conference on File and Storage Technologies (FAST '27) **[2x]** |
| Dates | February 23–25, 2027 **[2x]** |
| Venue | Hyatt Regency Lake Washington, Renton, WA, USA **[2x]** |
| Submission site | HotCRP, linked from the CFP **[inf]** |

## Deadlines — Fall cycle (the one that is still open)

FAST '27 ran **two** submission deadlines. The Spring deadline (Tue Mar 17, 2026)
has passed. Only the Fall cycle remains, and there is no later FAST '27 cycle —
the next opportunity after this is FAST '28.

| Milestone | Date | Conf. |
|---|---|---|
| **Paper submission** | **Tue Sept 15, 2026, 23:59 AoE** | **[2x]** |
| Author response period opens | Tue Nov 17, 2026 | [1x] |
| Author response period closes | Thu Nov 19, 2026, 23:59 AoE | [1x] |
| Notification to authors | Tue Dec 8, 2026 | [2x] |
| Artifact submission | Thu Dec 17, 2026, 23:59 AoE | [1x] |
| Final paper files due | Tue Jan 26, 2027, 23:59 AoE | [1x] |

**As of 2026-09-07 the submission deadline is 8 days away.**

> Note on a stale figure: some search results surface a Jun 4 2026 notification /
> Jun 16 2026 artifact deadline / Jul 28 2026 final-files date. Those belong to
> the **Spring** cycle, which has already run. Do not use them.

AoE (Anywhere on Earth) = UTC−12. A Sept 15 AoE deadline expires at **12:00 UTC on
Sept 16**, i.e. 08:00 EDT / 05:00 PDT Sept 16. USENIX does not grant extensions.

## Submission requirements

| Requirement | Value | Conf. |
|---|---|---|
| Long paper | ≤ **12 pages**, excluding references | **[2x]** |
| Short paper | ≤ **6 pages**, excluding references | **[2x]** |
| Page size | US letter | [1x] |
| Columns | Two | [1x] |
| Body font | 10pt Times Roman on 12pt leading, single-spaced | [1x] |
| Text block | 7" wide × 9" deep | [1x] |
| Template | USENIX LaTeX template + style file, from the USENIX templates page | [1x] |
| Format | PDF | [inf] |

Short papers are reviewed against the same bar as long papers — a short paper is
a *complete, smaller* contribution, not a partial long paper. FAST does not treat
6 pages as a lower-quality tier.

## Double-blind

FAST '27 is **fully double-blind**; all submissions must comply. **[2x]**

- Authors must not be identified explicitly *or by implication*.
- Refer to your own prior work in the third person, as you would anyone else's.
- **Do not write "reference removed for blind review."**
- Supplemental material must also be anonymized.
- Deployed-systems exception: the product or company *described* in the paper need
  not be anonymized — but the author names still must be.
- **Submissions violating anonymization will not be reviewed.** This is a
  desk-reject, not a fixable review comment.

Anonymization is the single largest mechanical risk for DB25, whose repos,
paper PDFs, and commit history all carry the author's name and `space-rf-org`
throughout. See [`submission-checklist.md`](submission-checklist.md) §2.

## Policies

- **No simultaneous submission.** The same work may not be under review at
  another venue concurrently. Submitting previously published work, or
  plagiarism, is treated as fraud and USENIX may act against the authors. **[2x]**
- The existing `db25-tokenizer-paper.pdf` and `arena_allocator_paper.pdf` in the
  repos — if they have been published or are under review anywhere — constrain
  what can be submitted here. If they are unpublished drafts sitting in a public
  GitHub repo, that is *not* prior publication, but see the anonymization note
  above: a public repo containing the paper text is a de-anonymization vector.

## Scope, in the PC's own words

> "The topics of interest to FAST are various aspects of systems related to
> storage, including both core storage topics and the application of storage to
> different application domains. The program committee interprets storage-related
> systems broadly: submissions on low-level storage devices, distributed storage
> systems, information and data management, as well as other systems
> interconnected with storage are all of interest." **[2x]**

"Information and data management" is the clause a database paper would have to
live under. It is real, but it is the narrowest door in that sentence, and the PC
composition is overwhelmingly storage-systems researchers. See
[`venue-fit.md`](venue-fit.md).

## Artifact Evaluation

FAST '27 runs a separate Call for Artifacts with its own deadline (Dec 17, 2026
for the fall cycle) and its own committee. AE happens **after** acceptance, so it
does not consume any of the next 8 days — but designing for it now costs nothing
and DB25 is unusually well positioned. See
[`artifact-evaluation.md`](artifact-evaluation.md).

## Sources

- [FAST '27 Call for Papers](https://www.usenix.org/conference/fast27/call-for-papers)
- [FAST '27 conference page](https://www.usenix.org/conference/fast27)
- [FAST '27 Call for Artifacts](https://www.usenix.org/conference/fast27/call-for-artifacts)
- [FAST '27 CFP PDF](https://www.usenix.org/sites/default/files/fast27_cfp_031126.pdf)
- [USENIX Calls for Papers index](https://www.usenix.org/conferences/calls-for-papers)
- [FAST '26 Double-Blind Guidance](https://www.usenix.org/conference/fast26/double-blind-guidance) (the '27 guidance page follows the same text)
