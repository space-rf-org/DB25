# DB25 Parser Conformance Oracle (bootstrap)

The shared *one SQL → one strict outcome* validation set for **both** parser tracks
(the existing hand-written parser we keep + fix, and the community-challenge
formally-verifiable parser). See
[`../docs/formally-verifiable-parser-challenge.md`](../docs/formally-verifiable-parser-challenge.md)
for the why and the tracks; this directory is the *what to validate against*.

> **Bootstrap status.** The canonical format below is real and testable *today*
> (frozen goldens were produced by `tools/ast_to_sexpr`). Everything marked **TODO**
> is deliberately deferred. Nothing is wired into CMake/CI yet.

---

## 1. The contract

Parsing is a **total function**:

```
outcome(sql) := ACCEPT(canonical_ast)   -- exactly one, stable, byte-for-byte
             |  REJECT(diagnostic)       -- precise class (+ position: TODO)
```

`ACCEPT` requires the **whole input consumed** and **no recorded error**; anything
else is `REJECT`. There is no third state (no "success with a partial tree") — that
third state is exactly the defect this oracle exists to forbid (issue
[#71](https://github.com/space-rf-org/db25-sql-parser/issues/71)).

Two conforming parsers MUST emit byte-identical `canonical_ast` for the same SQL.

---

## 2. Canonical AST format — S-expression

One fully-parenthesized, single-line S-expression per tree. Deterministic and
diffable; independent of any parser's in-memory layout.

```
node    := "(" name payload? attr* child* ")"
name    := grammar-level node kind            ; e.g. SelectStmt, BinaryExpr, ColumnRef
payload := STRING                             ; the node's primary text (operator, literal, identifier)
attr    := ":" key " " STRING                 ; e.g. :schema "s"  :catalog "c"
child   := node                               ; in SOURCE order, never reordered
STRING  := '"' (char | '\"' | '\\')* '"'      ; backslash-escape " and \
```

Rules:
1. **Order is significant and fixed** — children appear in source/grammar order.
2. **Single line, single space** between tokens; no trailing space. (Pretty-printing
   is a viewer concern, never the canonical form.)
3. **Payload omitted when empty** (e.g. `(Star)`, `(SelectList …)`).
4. **Attributes sort in a fixed order**: `:schema` before `:catalog` (extend the fixed
   order as new attrs are added — never emit in insertion order).

### Verified examples (frozen goldens, `positive/goldens.sexpr`)

```
SELECT id, name FROM users WHERE id = 1
=> (SelectStmt (SelectList (ColumnRef "id") (ColumnRef "name")) (FromClause (TableRef "users")) (WhereClause (BinaryExpr "=" (ColumnRef "id") (IntegerLiteral "1"))))

SELECT a + b * c FROM t
=> (SelectStmt (SelectList (BinaryExpr "+" (ColumnRef "a") (BinaryExpr "*" (ColumnRef "b") (ColumnRef "c")))) (FromClause (TableRef "t")))
```

### Canonicalization TODOs (bootstrap emits DB25 node names verbatim)

The bootstrap converter prints the DB25 `NodeType` spelling and copies the parser's
fields directly, which exposes parser-specific quirks the *normative* format must
normalize away:

- [ ] **Formalism-neutral vocabulary.** Map DB25 spellings to a parser-agnostic set
      (e.g. `(BinaryExpr "=" …)` → `(= …)`, `(IntegerLiteral "1")` → `(Int 1)`), so a
      Track-B parser with different internal names still matches.
- [ ] **Field-overloading leaks.** `SELECT COUNT(*) AS n` currently serializes the
      alias into `:schema "n"` (the parser reuses `schema_name` for the alias). The
      canonical form must model an alias as its own construct, not a schema attr.
- [ ] **Dotted names.** `c.s.t.col` currently lands whole in `primary_text`, and a
      table `c.s.t` splits as `primary_text="t" :catalog "c.s"`. Canonical form must
      represent qualified names as an explicit, ordered part list.
- [ ] **Literal typing.** Decide whether numeric/string/bool literals carry a typed
      tag vs a raw payload string.

---

## 3. Corpus files

| File | Kind | Row = | Status |
|---|---|---|---|
| `positive/goldens.sexpr` | POSITIVE | `sql → canonical AST` | seed (5 verified) |
| `negative/pathologies.tsv` | NEGATIVE | `malformed sql → REJECT + class` | **12 rows, lifted from #71** |
| `positive/feature-gaps.tsv` | POSITIVE (TODO trees) | valid sql the parser truncates today | 10 rows, expected-AST TODO |

### `negative/pathologies.tsv`
Genuine syntax errors whose required outcome is `REJECT`. Today the parser ACCEPTs
them with a silently truncated AST (the `current_behavior` column records what).
Columns: `id  diag_class  sql  current_behavior`. `expect = REJECT` for every row.
**TODO:** pin exact diagnostic position per row once the strict contract lands.

### `positive/feature-gaps.tsv`
The *accept-but-wrong* rows of #71: valid SQL (or should-be) that is silently
truncated or mis-parsed (`DROP TABLE s.t`, `CREATE INDEX … DESC`, `RETURNING id+1`,
`-a::int`, `array_agg(x ORDER BY y)`, …). These belong in the positive corpus, but
their correct expected tree is **TODO** — the current reference parser cannot yet
produce it, so they are kept separate rather than freezing a wrong golden. One row
(`F09` caret precedence) is flagged `DECIDE` — dialect-dependent.

### Not built yet
- [ ] **Positive corpus at scale.** Add a canonical-AST column to the umbrella
      `corpus/corpus.tsv` (325 curated rows) — today it records accept/analyze tags
      + provenance but not the tree.
- [ ] **Totality property harness.** Grammar-derived + mutated + fuzzed inputs
      asserting *every* input is ACCEPT-or-REJECT — never partial/crash/hang. This is
      the machine-checkable "no third state" test; seed off the umbrella
      `db25_harness` / `db25_gate`.
- [ ] **Result equivalence.** Run ACCEPT trees through the umbrella reference
      evaluator so "same tree" is also checked by "same result".

---

## 4. Tools

### `tools/ast_to_sexpr.cpp` — reference canonical serializer
Links the existing DB25 parser as a *reference* producer and prints the canonical
S-expr (`ACCEPT`) or `REJECT <message>`. It is the "converter from the existing
dumper" — modeled on `db25-sql-parser/tools/ast_dumper.cpp` but emitting the
parser-agnostic canonical form instead of a debug view.

**Build & run** (against a built `db25-sql-parser` checkout at `$P`):
```sh
P=/path/to/db25-sql-parser        # must have build/libdb25parser.a, build/libdb25arena.a
g++ -std=c++23 -O2 -fno-exceptions \
    -I "$P/include" -I "$P/external/tokenizer/include" \
    tools/ast_to_sexpr.cpp "$P/build/libdb25parser.a" "$P/build/libdb25arena.a" \
    -o tools/ast_to_sexpr

tools/ast_to_sexpr "SELECT id FROM t WHERE id = 1"     # prints the canonical S-expr
tools/ast_to_sexpr "SELECT 1 UNION"                    # currently ACCEPTs (the bug) — see #71
```

**TODO:** wire into CMake; add a `run_corpus` driver that (a) diffs `positive/*`
goldens and (b) asserts every `negative/*` row is `REJECT`; add a `make goldens`
regeneration target; add the totality fuzzer.

---

## 5. How each track uses this

- **Track A (fix the existing parser):** after the strict-contract fix, every
  `negative/pathologies.tsv` row must flip to `REJECT`, and `positive/goldens.sexpr`
  must stay byte-identical. That diff *is* the acceptance test for the fix.
- **Track B (challenge entry):** must emit byte-identical `positive` goldens, `REJECT`
  every `negative` row, and pass the totality harness — independent of its
  architecture.
