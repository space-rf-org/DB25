# Paper scaffolding

| File | What it is |
|---|---|
| `fast27-skeleton.tex` | USENIX-format, anonymized LaTeX skeleton with section-by-section writing notes |
| `abstracts.md` | Drafted abstracts for each of the three candidate framings |

## To start writing

```sh
mkdir -p ~/fast27-paper && cd ~/fast27-paper
cp /path/to/DB25/docs/fast27/paper/fast27-skeleton.tex paper.tex
# Download usenix2019_v3.sty (or the current version) from the USENIX
# author-resources / paper-templates page into this directory.
touch refs.bib
pdflatex paper && bibtex paper && pdflatex paper && pdflatex paper
exiftool -all= -overwrite_original paper.pdf && pdfinfo paper.pdf   # verify anonymous
```

**Write the paper outside these repos.** A draft committed to a public
`space-rf-org` repository is a de-anonymization vector for any double-blind
venue, and it stays in the history after you delete it.

## Two things the skeleton cannot do for you

1. **The style file.** It is not vendored here — download the current one from
   USENIX rather than reusing the IEEEtran setup in
   `DB25-sql-tokenizer/papers/`, which does not meet USENIX's format rules.
2. **The bibliography.** `refs.bib` starts empty. There are currently zero
   citations anywhere in the DB25 documentation, so related work is a full day
   of real reading — see the coverage list in the skeleton's Related Work
   section.
