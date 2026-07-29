# DB25 — A Guided Tour of the Whole Engine

This is an umbrella-level tutorial: it shows how the **DB25 SQL engine as a whole**
turns a SQL string into a logical query plan, one stage at a time, using five
queries of increasing difficulty. Every plan and result shown here is produced by
the runnable programs in [`../examples/`](../examples/) — nothing is hand-waved.

> **Where the engine stands today.** DB25 is built one repository per stage. As of
> this writing the stack is complete from the raw SQL text through the *optimized
> logical plan*; there is **no physical planner or execution engine yet**. So the
> real output of the engine is a **logical plan**. To let this tutorial show actual
> result rows, the umbrella repo ships a **reference evaluator** (a test aid, not the
> engine) that runs a logical plan over small in-memory tables with correct
> three-valued (NULL) logic. It is deliberately partial — see
> [§7](#7-the-reference-evaluator-and-what-it-does-not-do).

---

## 1. The pipeline

A query flows through five stages. Each is its own repository; the umbrella repo
(`db25`) wires them together and adds the end-to-end harness.

```
   SQL text
      │
      ▼
┌─────────────┐   1. Tokenizer   (DB25-sql-tokenizer)
│  tokenize   │      SIMD-assisted lexer → a flat stream of typed tokens
└─────────────┘      (keywords, identifiers, numbers, strings, operators)
      │
      ▼
┌─────────────┐   2. Parser      (db25-sql-parser)
│    parse    │      Pratt/recursive-descent → an arena-allocated AST
└─────────────┘      (SelectStmt → SelectList / FromClause / WhereClause / …)
      │
      ▼
┌─────────────┐   3. Analyzer    (DB25-Semantic-Analyzer)
│   analyze   │      resolves names against the catalog; infers a DataType and a
└─────────────┘      2-bit nullability for every node; reports diagnostics
      │
      ▼
┌─────────────┐   4. Binder      (db25-logical-plan)
│    bind     │      lowers the analyzed AST into a relational-algebra tree of
└─────────────┘      LogicalNodes with owned, typed Expr trees
      │
      ▼
┌─────────────┐   5. Optimizer   (db25-logical-plan)
│  optimize   │      rewrites the plan preserving results (column pruning,
└─────────────┘      predicate pushdown, …) → the optimized logical plan
      │
      ▼
  (physical planner / executor — planned; a reference evaluator stands in here)
```

The **binder is a consumer of the analyzer**: it never re-derives types or resolves
names — it reads the analyzer's results plus the catalog and assembles the pipeline

```
   Scan → [Join] → [Filter (WHERE)] → [Aggregate (GROUP BY)]
        → Project (SELECT list) → [Sort (ORDER BY)] → [Limit]
```

(bracketed stages appear only when the corresponding clause is present).

---

## 2. Building and running

```sh
git clone --recurse-submodules https://github.com/space-rf-org/db25
cd db25
git submodule update --init --recursive
cmake -S . -B build && cmake --build build -j
```

Then run the two tour programs:

```sh
./build/examples/db25_tour               # the five queries below, end to end
./build/examples/db25_dialect_features   # the six SQL-dialect features
```

The umbrella entry point both use is one function:

```cpp
db25::harness::Stages s = db25::harness::run(catalog, sql);
//  s.parsed / s.analyze_ok / s.bind_ok / s.optimize_ok  — per-stage success
//  s.bound      — the logical plan straight out of the binder
//  s.optimized  — the plan after optimize()
//  s.bound_dump / s.optimized_dump — printable renderings

auto rows = db25::harness::eval(s.optimized, data);   // reference evaluator
```

## 3. Reading a plan dump

Plans print bottom-up-nested. Each line is one operator; indentation is
parent → child. Two bracket styles appear:

- `(… )` after an operator is its **expression payload** — a projection list, a
  filter predicate, a join condition. A column is written `#N:Type`, a positional
  index into the operator's *input* row (child outputs concatenated). `Type?` means
  nullable.
- `[… ]` is the operator's **output schema** (`name:Type?`).

So `Filter (#2:Integer >= 18:Integer):Boolean` reads: *keep rows where input
column #2 (an Integer) is ≥ the integer literal 18; the predicate is Boolean.*

---

## 4. The sample database

Every example runs against this fixed catalog and rows (defined in
[`../harness/harness.cpp`](../harness/harness.cpp)). NULLs, duplicate cities, a
user with no orders, and an order whose `user_id` has no user are all present on
purpose, so the three-valued logic actually gets exercised.

**`users`**

| id | name  | age  | city | manager_id |
|----|-------|------|------|------------|
| 1  | alice | 30   | NYC  | NULL |
| 2  | bob   | NULL | LA   | 1 |
| 3  | carol | 5    | NYC  | 1 |
| 4  | dave  | 40   | LA   | 2 |
| 5  | eve   | 25   | NULL | 2 |

**`orders`** (`id, user_id, product_id, amount, status`): 100→(1,10,50,paid),
101→(1,11,5,paid), 102→(2,12,30,pending), 103→(3,13,NULL,paid),
104→(NULL,10,8,paid), 105→(4,12,120,NULL), 106→(9,10,40,shipped).

Also `products(id,name,price,category)` and `emp(id,name,mgr_id,dept_id,salary)`.

---

## 5. Five queries, increasing difficulty

### Level 1 — Scan + Project · *walked through every stage*

```sql
SELECT id, name FROM users
```

**Stage 1 — Tokenize.** The lexer emits a flat token stream (whitespace dropped):

| token | `SELECT` | `id` | `,` | `name` | `FROM` | `users` |
|-------|----------|------|-----|--------|--------|---------|
| kind  | Keyword | Identifier | Delimiter | Identifier | Keyword | Identifier |

**Stage 2 — Parse.** Tokens become an AST:

```
SelectStmt
├─ SelectList
│  ├─ ColumnRef "id"
│  └─ ColumnRef "name"
└─ FromClause
   └─ TableRef "users"
```

**Stage 3 — Analyze.** Each `ColumnRef` is resolved against `users` in the catalog
and stamped with a type and nullability: `id → Integer, NOT NULL`;
`name → Text, nullable`. No diagnostics.

**Stage 4 — Bind.** The analyzed AST lowers to a two-node logical plan — a
`Project` over a base-table `Scan`. The projected items become positional column
references (`#0`, `#1`) into the scan's output:

```
Project (#0:Integer, #1:Text) [id:Integer, name:Text?]
  Scan users [id:Integer, name:Text?, age:Integer?, city:Text?, manager_id:Integer?]
```

**Stage 5 — Optimize.** Nothing needs reordering, but the optimizer's **column
pruning** notices the scan only needs `id` and `name`, and trims the scan's output
schema from five columns to two:

```
Project (#0:Integer, #1:Text) [id:Integer, name:Text?]
  Scan users [id:Integer, name:Text?]          ← pruned from 5 columns to 2
```

**Result** (reference evaluator over the sample rows):

```
1 | 'alice'
2 | 'bob'
3 | 'carol'
4 | 'dave'
5 | 'eve'
(5 rows)
```

That is the whole pipeline. The next four levels each add one idea; we show the
new stage/operator and the plans, and lean on the pattern above for the rest.

### Level 2 — Filter (WHERE) with three-valued logic

```sql
SELECT name, age FROM users WHERE age >= 18
```

The `WHERE` clause introduces a **`Filter`** between the scan and the project. Its
predicate is an owned, typed `BinaryOp`; the `18` is an integer literal.

```
Bound:                                      Optimized (column-pruned):
Project (#1:Text, #2:Integer)               Project (#0:Text, #1:Integer)
  Filter (#2:Integer >= 18:Integer):Boolean   Filter (#1:Integer >= 18:Integer):Boolean
    Scan users [ …5 cols… ]                     Scan users [name:Text?, age:Integer?]
```

**Three-valued logic in action.** `age >= 18` is `UNKNOWN` for bob (his age is
NULL), and SQL keeps a row only when the predicate is *true* — so bob is dropped,
not kept. carol (age 5) fails the comparison. Result:

```
'alice' | 30
'dave'  | 40
'eve'   | 25
(3 rows)
```

### Level 3 — Aggregate (GROUP BY)

```sql
SELECT city, COUNT(*) AS n FROM users GROUP BY city
```

`GROUP BY` lowers to an **`Aggregate`** node whose output is *group keys followed by
aggregates*. Note how the binder threads the grouping column and the `COUNT(*)`
call through, and the optimizer prunes the scan to just `city`:

```
Bound:                                          Optimized:
Project (#0:Text, #1:BigInt)                     Project (#0:Text, #1:BigInt)
  Aggregate group=(#3:Text) aggs=(COUNT():BigInt)  Aggregate group=(#0:Text) aggs=(COUNT():BigInt)
    Scan users [ …5 cols… ]                          Scan users [city:Text?]
```

The plan is exactly right: three groups (`NYC`, `LA`, and one for the NULL city),
with a `COUNT(*)` per group. The reference evaluator groups the rows (NULL is its
own group, `NULL == NULL` for grouping) and computes the per-group count:

```
'NYC' | 2
'LA'  | 2
NULL  | 1
(3 rows)
```

The **logical plan is the deliverable** at this stage of the project; the evaluator
is a test stand-in (§7) that nonetheless computes grouped-aggregate values, so the
counts above are real, not placeholders.

### Level 4 — Join + predicate pushdown (with a hex literal)

```sql
SELECT u.name, o.amount
FROM users u JOIN orders o ON u.id = o.user_id
WHERE o.amount >= 0x0A
```

Two things to see. First, the **hex literal** `0x0A` is lexed as one number token
and lowered to the integer **10** — visible as `>= 10` in the plan. Second, the
optimizer performs **predicate pushdown**: `o.amount >= 10` touches only `orders`,
so it moves *below* the join, onto the `orders` scan, shrinking the join's inputs.

```
Bound:                                        Optimized:
Project (#1:Text, #8:Integer)                  Project (#1:Text, #3:Integer)
  Filter (#8:Integer >= 10:Integer):Boolean      Join (INNER) ON (#0 = #2):Boolean
    Join (INNER) ON (#0 = #6):Boolean              Scan users AS u [id, name]
      Scan users AS u [ …5 cols… ]                 Filter (#1:Integer >= 10):Boolean
      Scan orders AS o [ …5 cols… ]                  Scan orders AS o [user_id, amount]
```

The join condition and filter index into the **concatenated** input schema (users'
columns then orders'), which is why the same column is `#8` before pushdown and
`#3` after the scans are pruned. Result (the inner join drops the order whose
`user_id` 9 has no user; amounts ≥ 10 keep alice/bob/dave):

```
'alice' | 50
'bob'   | 30
'dave'  | 120
(3 rows)
```

### Level 5 — Correlated scalar subquery (with a hex literal)

```sql
SELECT u.name,
       (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS orders
FROM users u
WHERE u.age >= 0x12
```

The projection contains a **correlated scalar subquery**: a per-row `COUNT` of a
user's orders. The binder lowers it into an owned sub-plan hanging off the
`Project`, and — crucially — the correlation `o.user_id = u.id` becomes a
first-class **`OuterRef`** (`outer1#0`: depth 1, the enclosing block's column #0).
The hex literal `0x12` in the outer filter is the integer **18**.

```
Project (#1:Text, Subquery[SCALAR,correlated]:BigInt) [name:Text?, orders:BigInt?]
  Filter (#2:Integer >= 18:Integer):Boolean
    Scan users AS u [ …5 cols… ]
  SubPlan (scalar, correlated)
    Project (#0:BigInt)
      Aggregate group=() aggs=(COUNT():BigInt)
        Filter (#0:Integer = outer1#0:Integer):Boolean   ← the correlation
          Scan orders AS o [user_id]
```

The optimizer column-prunes inside the sub-plan (the inner scan collapses to just
`user_id`); this particular correlated scalar subquery stays a sub-plan rather than
being flattened into a join. The reference evaluator runs it with nested-loop
semantics — for each outer user passing `age >= 18`, it recomputes the inner
`COUNT`. The inner `COUNT(*)` is a *scalar* aggregate, so it evaluates:

```
'alice' | 2      ← alice (30) has orders 100, 101
'dave'  | 1      ← dave  (40) has order 105
'eve'   | 0      ← eve   (25) has no orders
(3 rows)
```

(bob and carol are filtered out: bob's age is NULL → UNKNOWN, carol's age 5 < 18.)

---

## 6. Dialect features the stack accepts today

Beyond textbook SQL, the stack handles six dialect features end to end (each
verified by `db25_dialect_features`):

| # | Feature | Example | What happens |
|---|---------|---------|--------------|
| 1 | Hex / binary integers, leading-dot floats | `0xFF`, `0b1010`, `.5` | one number token → integer 255 / 10, double 0.5 |
| 2 | Delimited (double-quoted) identifiers | `"id"`, `"user name"` | a column named exactly that (case/space preserved; keywords allowed) |
| 3 | Single-quote `''` escape | `'O''Brien'` | value `O'Brien` (the doubled quote collapses) |
| 4 | CAST type modifiers | `CAST(x AS DECIMAL(10,2))` | precision 10 / scale 2 carried on the Cast |
| 5 | `ARRAY[]` constructor | `ARRAY[1,2,3]` | lowered to a `ScalarFunction "ARRAY"` typed `Array` |
| 6 | `COLLATE` annotation | `x COLLATE "C"` | a tight postfix → `ScalarFunction "COLLATE"(x, 'C')` |

All six compose in a single statement. Here is the actual **bound plan** for one
query that uses every feature at once (from `db25_dialect_features`):

```sql
SELECT CAST("id" AS DECIMAL(10,2)), ARRAY[0xFF, 0b1010, .5], "name" COLLATE "C"
FROM users
WHERE "name" COLLATE "C" = 'O''Brien' AND "id" = 0xFF
```

```
Project (CAST(#0:Integer AS Decimal),
         ARRAY(255:Integer, 10:Integer, 0.500000:Double):Array,
         COLLATE(#1:Text, 'C':Text):Text)
  Filter ((COLLATE(#1:Text, 'C':Text):Text = 'O'Brien':Text):Boolean
          AND (#0:Integer = 255:Integer):Boolean):Boolean
    Scan users [id, name, age, city, manager_id]
```

Read it and you can see every feature landed: `"id"`/`"name"` resolved to columns
`#0`/`#1`; `0xFF` → `255` and `0b1010` → `10` and `.5` → `0.5`; the `CAST` to
`Decimal`; `ARRAY(…)` and `COLLATE(…)` as scalar-function nodes; and `'O''Brien'`
collapsed to `'O'Brien'`.

---

## 7. The reference evaluator, and what it does *not* do

The `eval()` in the harness exists so tests can check *result equivalence*
(`eval(bound) == eval(optimized)`) rather than just plan shape. It is a reference
stand-in for the executor DB25 does not have yet, and it is intentionally partial:

- It returns `std::nullopt` for a plan shape it does not implement (a first-class,
  non-crashing "unsupported" outcome), so a differential test skips rather than lies.
- It implements scans, filters, projects, joins, sorts, limits, DISTINCT, scalar
  and grouped aggregates (`GROUP BY`, including a NULL group), and correlated
  subqueries (nested-loop semantics) with correct three-valued logic.

None of this reflects a gap in the **engine** — the engine's job today ends at the
optimized logical plan, and those plans are correct. The evaluator is a testing
aid, and the umbrella repo's real quality bar is the **falsifiability gate**
(`db25_gate`): the suite must pass clean, every injected mutant must be caught by at
least one test, and any test no mutant can break is reported as vacuous.

---

## 8. Where to go next

- Per-stage internals live in each stage's repo (tokenizer, parser, analyzer,
  logical-plan) — see the table in the umbrella [`README.md`](../README.md).
- [`docs/sql-surface.md`](sql-surface.md) — the SQL surface the whole stack accepts.
- [`docs/harness-findings.md`](harness-findings.md) — what the end-to-end harness
  caught on its first run.
- [`docs/engine-comparison-findings.md`](engine-comparison-findings.md) — DB25 vs.
  FoundationDB / DuckDB / Polars / SQLite.
- The two programs behind this tutorial: [`../examples/`](../examples/).
