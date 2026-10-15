# DB25 Dependency Composition — Design

**Status:** accepted · **Scope:** how the five DB25 repositories find, build and link each other · **Author:** Chiradip Mandal

---

## 1. Why (the one page)

DB25 is one program split across five git repositories:

```
DB25-sql-tokenizer  ->  db25-sql-parser  ->  DB25-Semantic-Analyzer
                                         \
                                          ->  db25-logical-plan  ->  db25-physical-plan  ->  db25
```

They are always built together, they are released together, and nothing outside
the chain consumes them. The split buys independent CI and a review boundary per
stage; it costs us a dependency-composition problem that a single repository
would not have. This document records how that problem is solved and **why the
alternatives were rejected**, because the answer was previously visible only as
an aside in one `CMakeLists.txt` comment — which is how it drifted.

**The decision in one sentence.** Every DB25 dependency is a **git submodule**,
consumed by **`add_subdirectory`**, built **`STATIC`**, with **no install step**
anywhere in the chain.

### 1.1 What "single binary" does and does not mean

It is worth being exact, because the phrase is used loosely and the loose
reading is wrong.

**True:** no DB25 code is ever in a shared library, and no part of the chain is
consumed as an installed artifact. Every target is `STATIC`, the tokenizer
forces `BUILD_SHARED_LIBS OFF`, and each executable carries the whole frontend
inside it. `ldd` on a built binary resolves nothing but the C/C++ runtime
(`libstdc++`, `libm`, `libc`, `libgcc_s`) — zero DB25 objects — and no `.so` is
produced anywhere in the build tree.

**Not true:** that the build produces one executable. The umbrella build emits
nine (`db25_harness`, `db25_gate`, `corpus_runner`, `stage_timer`, …) plus the
test targets, over four intermediate static archives.

So the property we actually hold is **statically linked, self-contained
binaries, no install**. The archives exist; they are link-time inputs, not
deployables.

---

## 2. Design decisions

### D1 — Submodules, because the consumption model needs source at configure time

Dependencies are compiled **in-tree**: `add_subdirectory(external/parser)`, and
the resulting objects land in the consumer's own archive. That requires the
dependency's **source tree present at configure time, offline, at an exact
recorded version**. Four mechanisms can supply a dependency; only one supplies
all three:

| mechanism | source at configure | offline | version pinned in git |
|---|---|---|---|
| **git submodule** | yes | yes | yes — a gitlink |
| `FetchContent` | yes | **no** — network | a SHA in CMake text |
| `find_package` + install | **no** — needs an install step | yes | **no** |
| vendored copy | yes | yes | **no pin at all** |

The other three are not hypothetical rejections; the tree shows them being
rejected. `FetchContent` appears exactly once, for googletest in the parser's
test build, and `db25-logical-plan`'s CMake says plainly that it "does not
FetchContent googletest (no network)". The install path is discussed in D3.

**The gitlink is the part that earns its keep.** A submodule pin is a tree
object: it diffs, `git submodule status` reads it, and a script can compare two
of them. `scripts/check_pin_agreement.sh` exists precisely because it can —
it catches the umbrella and db25-physical-plan drifting to different
`db25-logical-plan` commits, which would otherwise ship a pairing no CI had
ever built. A pin that is not a gitlink cannot be checked that way, and the one
place in this project that tried it proves the point: the semantic analyzer's
CI pins the parser with `PARSER_SHA` in a YAML file, and it was found sitting
three commits behind the parser's `main` with nothing noticing. (That number is
a snapshot and will be wrong by the time you read it, which is the point: no
check recomputes it.)

### D2 — `add_subdirectory`, and the `NOT TARGET` guard

A repository that may be built standalone **or** inside a superproject must
tolerate both. The idiom, used everywhere in the chain:

```cmake
if(NOT TARGET db25logicalplan)
    add_subdirectory(external/db25-logical-plan)
endif()
```

The superproject adds its copy first; the child sees the target and reuses it
instead of adding a second, colliding one. Two consequences follow, and both
are deliberate:

- **A child's own nested submodule is never built by a parent.** It is cloned
  by `--recursive` and then ignored. db25-physical-plan's nested
  `db25-logical-plan` can be left uninitialised entirely and the umbrella still
  configures, builds, and passes 18/18.
- **A child must not resolve a dependency by a relative path.** It must go
  through the target, because the parent's copy is somewhere else.
  db25-physical-plan does this correctly, reading the logical-plan IR headers
  off `INTERFACE_INCLUDE_DIRECTORIES` rather than `../..`.

### D3 — One name for a target, whichever way it arrived

A library that is both installed and added in-tree must present the **same
target name** on both paths. The parser publishes `DB25::db25parser` from its
export, so it also aliases its in-tree target to that name:

```cmake
add_library(DB25::db25parser ALIAS db25parser)
```

Without the alias the two paths disagreed, and the fallout was not theoretical.
`DB25-Semantic-Analyzer` links `DB25::db25parser` and could therefore only be
built through an install, which meant no parent could `add_subdirectory` it.
Both parents worked around that by **compiling the analyzer's `.cpp` files
directly into their own targets** — `db25-logical-plan` naming two of them, the
umbrella naming four, out of five, with nothing checking either list against
the repo. A new translation unit in the analyzer was silently missing from both
parents until a link failed. That had already happened once; the evidence is a
note-to-self comment in logical-plan's CMake.

**A usage requirement must hold on both paths too.** The same exercise found
`db25parser` exposing its own `include/` and not the tokenizer's, even though a
public parser header `#include`s a tokenizer header. The install hid it
completely, because `install(DIRECTORY ${TOKENIZER_DIR}/include/)` flattens
both trees into one prefix. An in-tree consumer failed to compile on exactly
that header. The rule this yields:

> If a target is consumed both ways, every usage requirement must be stated on
> the target. Anything that works only because the install flattens or copies
> something is a difference between the two paths, and it will be found by
> whichever path is tried second — possibly years later.

### D4 — An install is for consumers OUTSIDE the chain

`db25-sql-parser` is the only repository with `install(EXPORT)` and a
`Config.cmake`, and it keeps them: they are what a third party would use. But
**no DB25 repository consumes another through an install.** Requiring one
inside the chain costs a build step, produces a second copy of a library the
superproject already has, and admits the possibility that the two copies are
different builds of different commits.

---

## 3. The rule, so we do not drift back

> **A DB25 repository declares its dependencies as git submodules under
> `external/`, consumes them with `add_subdirectory` behind a `NOT TARGET`
> guard, and links them as `STATIC` targets. It does not require an install of
> another DB25 repository, does not resolve one by relative path, and does not
> compile another repository's sources by naming them.**

Naming another repository's translation units in your own `add_library` is the
specific anti-pattern this document exists to prevent. It is always a symptom:
the dependency could not be added as a subdirectory, and the real fix is in the
dependency's CMake, not in a hand-maintained source list.

---

## 4. Known costs, accepted

- **The checkout holds a duplicate.** `--recursive` clones
  `db25-logical-plan` and its children twice, once under the umbrella and once
  under db25-physical-plan. It is never built. `scripts/bootstrap.sh` does a
  selective init (`--recursive` for logical-plan, plain `--init` for
  physical-plan) and removes it; the build and `check_pin_agreement` are
  unaffected, because the pin check reads gitlinks via `git ls-tree HEAD`
  rather than the working tree. Measured on clean clones:

  |  | `external/` | whole checkout |
  |---|---|---|
  | `--init --recursive` | 15 MB | 41 MB |
  | `scripts/bootstrap.sh` | 7.6 MB | 24 MB |

  (An earlier revision of this document said "6.6 MB of a 33 MB checkout". 6.6
  MB is the duplicate *working tree* only — it omitted the git objects behind
  it under `.git/modules`, which a recursive init also fetches — and the 33 MB
  was measured on a tree that had been built in. Both numbers are replaced by
  the table above.)
- **A diamond, and therefore pin drift.** Two paths reach
  `db25-logical-plan`. `scripts/check_pin_agreement.sh` is the mitigation, and
  it is a `ctest` target so it cannot be forgotten.
- **Nesting is three deep** (umbrella -> logical-plan -> parser -> tokenizer).
  Flattening it would mean the umbrella declaring all five directly, but each
  repository must still build standalone and so keeps its own submodule anyway
  — the nesting would be unused rather than gone. Not worth the churn.

These are costs of the five-repository split, not of the composition mechanism.
A monorepo would remove all three and the gitlink pins with them. The split is
kept for its CI and review boundaries; this document is the price list.
