# Arena Allocator — a stack-wide allocation strategy

This paper documents the **arena / cache-aligned bump allocation** strategy DB25
uses for hot, short-lived, tree-shaped data. It lives here (not in a single stage)
because the technique is **cross-cutting**: the parser allocates its 128-byte AST
nodes from an arena, and the same discipline informs allocation choices elsewhere in
the stack (owned IR lifetimes, evaluator scratch buffers, and any future executor
morsel/columnar buffers).

- [`arena_allocator_paper.pdf`](arena_allocator_paper.pdf) — the paper (compiled artifact)
- [`arena_allocator_paper.tex`](arena_allocator_paper.tex) — LaTeX source
- [`compile_paper.sh`](compile_paper.sh) — build script (needs a TeX toolchain + gnuplot)

## Provenance & scope

- **Source:** relocated from `db25-sql-parser` (`docs/arena_allocator_paper.*`,
  commit `ee2ec99`), where the allocator was first built and measured.
- **The measured figures are parser-measured** (AST-node allocation). Read them as
  *characteristics of the strategy* — bump allocation, cache-line alignment, bulk
  free — not as stack-wide benchmark results. Any stage that adopts the strategy
  should re-measure in its own context.
- **Build note:** the PDF here is the artifact built in the parser repo; this
  environment has no `pdflatex`, so `compile_paper.sh` was **not** re-run during
  relocation. Rebuild it where a TeX toolchain is available before editing the source.

## Related

- [`../formal-methods-proposal.md`](../formal-methods-proposal.md) — where formal
  verification (TLA+/Alloy) fits across the stack; this paper is the allocation-strategy
  companion on the methodology shelf.
