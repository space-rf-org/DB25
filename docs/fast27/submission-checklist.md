# Submission checklist

Mechanical checks for a USENIX double-blind submission. Venue-independent except
where noted — most of this applies verbatim to CIDR, VLDB, and SIGMOD too.

Work top to bottom on the day before the deadline, not the day of.

---

## 1. Before you write a line

- [ ] Confirm every date and limit against the live CFP. This packet's figures
      were reconstructed from search results, not read off the page — see
      [`cfp-facts.md`](cfp-facts.md).
- [ ] Decide long (≤12pp) vs. short (≤6pp), excluding references. Short is not a
      lower bar at FAST; it's a smaller complete contribution.
- [ ] Confirm no part of the work is under review elsewhere. Simultaneous
      submission is treated as fraud by USENIX.
- [ ] Check whether the existing tokenizer / arena PDFs have been published or
      submitted anywhere. If so, they constrain what can be claimed as new.
- [ ] Download the current USENIX LaTeX template and style file from the USENIX
      author-resources page. Do not reuse an IEEEtran skeleton — the existing
      papers in these repos are IEEE-formatted and will not comply.

## 2. Anonymization — the desk-reject risk

FAST '27 is fully double-blind and violations **are not reviewed**. DB25 is
de-anonymized everywhere, so this needs a deliberate sweep rather than a glance.

- [ ] No author names, affiliations, or emails on the title page or in the PDF body.
- [ ] Strip `\author{}` content; the USENIX template has an anonymous mode — use it.
- [ ] Grep the source for: `Chiradip`, `Mandal`, `chiradip`, `Space-RF`,
      `space-rf`, `spacerf`, and your own email domain.
- [ ] **No GitHub URLs.** Every `github.com/space-rf-org/...` link de-anonymizes
      instantly. Replace with "[repository URL withheld for review]" or an
      anonymized mirror (anonymous.4open.science, or a fresh account).
- [ ] Cite your own prior work in the **third person**, exactly as you would
      someone else's: "DB25 [12] reports…", never "our previous work [12]" or
      "we previously showed."
- [ ] **Never write "reference removed for blind review."** The CFP calls this
      out specifically.
- [ ] Avoid naming the system if the name is uniquely searchable. "DB25" plus
      "SIMD SQL tokenizer" leads to a public repo with the author's name in the
      README, the commit log, and a PDF. Consider an anonymous system name for
      the submission and restore the real one for the camera-ready.
- [ ] Anonymize any supplemental material and any artifact link.
- [ ] Scrub PDF metadata — this is the most-missed step:
      ```sh
      pdfinfo paper.pdf                    # inspect Author / Creator / Producer
      exiftool -all= -overwrite_original paper.pdf   # strip everything
      pdfinfo paper.pdf                    # verify it's clean
      ```
      LaTeX writes the username into `/Creator` and `hyperref` writes `pdfauthor`.
      Set `\hypersetup{pdfauthor={},pdftitle={}}` and still verify after building.
- [ ] Check figures for embedded paths, hostnames, or usernames (matplotlib and
      TikZ both leak these), and screenshots for shell prompts showing a username.
- [ ] Have someone else read the PDF cold and try to name the authors.

## 3. Formatting

- [ ] US letter, two columns, 10pt Times Roman on 12pt leading, single-spaced.
- [ ] Text block 7" wide × 9" deep.
- [ ] Page count within limit **excluding references**; confirm what the CFP says
      about appendices — assume they count unless it says otherwise.
- [ ] No shrunken fonts in figures/tables to steal space. Reviewers notice, and
      USENIX has rejected papers for it.
- [ ] Figures legible in **greyscale** and when printed. Do not encode meaning in
      colour alone.
- [ ] All text in figures ≥ 8pt at final print size.
- [ ] Line numbers if the CFP requests them (some USENIX venues do).

## 4. Content self-review

- [ ] The abstract states the contribution and one headline result in numbers.
- [ ] The introduction says explicitly what is new, in a bulleted contributions list.
- [ ] **Every claimed number appears in a table or figure and is reproducible.**
- [ ] Baselines are real systems, measured by you on the same hardware and corpus
      — not numbers quoted from other papers. (See
      [`readiness-assessment.md`](readiness-assessment.md) §4(b).)
- [ ] Do the arithmetic on your own headline numbers before a reviewer does.
      (See §4(a) of the same file — this already bit the tokenizer paper.)
- [ ] Report variance: multiple runs, and either error bars or a stated
      run-to-run spread. Single-number results read as unserious.
- [ ] Hardware, OS, compiler, and flags stated precisely enough to reproduce.
- [ ] A limitations / threats-to-validity section that volunteers the real
      weaknesses. `docs/gap-register.md` is excellent raw material.
- [ ] Related work situates the paper against Cascades/Volcano, Orca, Calcite,
      DuckDB, Umbra, Photon, and (for lexing) simdjson, Mison, Sparser. 30–50
      references is normal; DB25's design docs currently cite none.
- [ ] Spell-check and a full read-aloud pass of the abstract and intro.

## 5. Submission day

- [ ] Register the abstract early if the venue requires a separate abstract
      deadline — check the CFP.
- [ ] Create the HotCRP account and start the submission **at least 24 hours
      early**. Site load spikes near AoE deadlines and the form asks for topic
      areas and conflicts you won't want to compose under pressure.
- [ ] Declare **conflicts of interest** accurately: advisors, advisees,
      co-authors within the standard window, same institution, close collaborators.
- [ ] Select topic areas honestly. Miscategorizing to dodge a scope problem
      earns a worse reviewer, not a better outcome.
- [ ] Upload, then **download your own submission from HotCRP and read it** —
      confirm it's the right file, renders correctly, and is still anonymous.
- [ ] Re-run `pdfinfo` on the downloaded copy.
- [ ] AoE = UTC−12. A Sept 15 AoE deadline expires **12:00 UTC Sept 16**
      (08:00 EDT / 05:00 PDT). Do not plan to use that margin.
- [ ] Tag the exact source commit used for the submission, in every repo, so the
      camera-ready and the artifact can be built from the same tree:
      ```sh
      git tag -a fast27-submission -m "State as submitted to FAST '27"
      ```

## 6. After submission

- [ ] Do not push the paper text or a de-anonymizing README to a public repo
      during review.
- [ ] Calendar the author-response window (Nov 17–19, 2026 for the FAST fall
      cycle) — it's short and easy to miss.
- [ ] Calendar the artifact deadline (Dec 17, 2026) and read
      [`artifact-evaluation.md`](artifact-evaluation.md) now, not then.
