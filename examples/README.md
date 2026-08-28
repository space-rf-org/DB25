# DB25 examples

Runnable programs that drive the **whole DB25 pipeline** (parse → analyze → bind →
optimize) and print what each stage produces. They accompany
[`../docs/tutorial.md`](../docs/tutorial.md) — every plan and result in the tutorial
comes from here.

Both programs reuse the umbrella harness translation unit
([`../harness/harness.cpp`](../harness/harness.cpp)) *without* its `main`, via the
`DB25_HARNESS_NO_MAIN` define, so they get `run()` / `eval()` / `catalog()` /
`data()` for free and stay consistent with the rest of the stack (`-fno-exceptions`).

| Program | Source | What it shows |
|---------|--------|---------------|
| `db25_tour` | [`tour.cpp`](tour.cpp) | Five queries of increasing difficulty (scan/project → filter → aggregate → join → correlated subquery), each with its bound plan, optimized plan, the optimized plan as a **canonical s-expression**, and the evaluated result. |
| `db25_dialect_features` | [`dialect_features.cpp`](dialect_features.cpp) | The six SQL-dialect features (hex/binary/`.5`, delimited identifiers, `''` escape, CAST modifiers, `ARRAY[]`, `COLLATE`) individually, then all six combined in one statement with its bound plan. |

## Seeing every stage as an s-expression, and the AST

`db25_tour` prints the optimized plan's s-expression via
[`db25::staged::plan_to_sexpr`](../integration/staged_sexpr.hpp). For **all five** layers
(tokens → AST → resolved AST → logical → optimized) of a representative statement, read a
committed staged fixture directly — each `-- <stage>` section is that stage's s-expr:

```sh
cat ../corpus/staged/03_group_by.fixture
```

For a **visual AST** (the node tree with guide lines) plus each stage's artifact as one
self-contained HTML page, use the umbrella's report renderer:

```sh
../build/corpus_report --html ../corpus/showcase.sql > report.html   # open in a browser
```

## Build & run

From the repository root:

```sh
git submodule update --init --recursive
cmake -S . -B build && cmake --build build -j
./build/examples/db25_tour
./build/examples/db25_dialect_features
```

## Writing your own

The whole entry point is one call. Point it at the shared catalog (or build your
own `db25::semantic::InMemoryCatalog`) and read the per-stage results off `Stages`:

```cpp
#include "harness/harness.hpp"
namespace h = db25::harness;

int main() {
    h::Stages s = h::run(h::catalog(), "SELECT id, name FROM users WHERE id = 0xFF");
    if (!s.bind_ok) { /* s.bind_error */ return 1; }

    printf("%s\n", s.bound_dump.c_str());       // logical plan from the binder
    printf("%s\n", s.optimized_dump.c_str());   // after optimize()

    if (auto rows = h::eval(s.optimized, h::data()))
        for (const auto& r : *rows) { /* r is a std::vector<h::Value> */ }
}
```

Add the source to [`CMakeLists.txt`](CMakeLists.txt) alongside the existing targets
(link `db25logicalplan`, compile the harness TU with `DB25_HARNESS_NO_MAIN`).
