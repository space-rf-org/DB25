# DB25 SQL Surface (declared, and checked)

What the **whole stack** does with each construct — tokenizer → parser → analyzer
→ binder → optimizer → physical lowering — as opposed to what the parser alone
accepts. The distinction matters: the parser is deliberately permissive, so
**parser-accepts ≠ stack-accepts**.

## This document is a spec, not a description

The table below is **executable**. `integration/surface_runner.cpp` parses it,
drives every row through the real pipeline, and fails CI when the claim and the
code disagree — **in either direction**:

- declared *further* than reality → the document promises what we do not do;
- declared *short of* reality → something started working and nobody revisited
  the claim.

That second direction is not hypothetical. Before this check existed, the
document called `JOIN … USING`, standalone `VALUES` and CTEs unsupported long
after all three lowered end to end, and called `NATURAL JOIN` semantically broken
after that was fixed — three understatements and an overstatement, in the one page
a reader consults to learn what DB25 does. Prose drifts; a checked artifact
cannot.

The check earns its keep immediately: writing this table, the author (correctly,
from the old page) claimed `GROUP BY 1` was rejected by design. The runner
disagreed, and the plan settles it — `GROUP BY 1` produces a plan **identical** to
`GROUP BY city`, resolving the ordinal to the select-list expression rather than
grouping by the literal. A stale claim, caught by the thing that exists to catch
stale claims, before it shipped.

There is deliberately **no `--update` mode**. A spec that rewrites itself to match
the code is a mirror, not a spec. The value is that a person wrote the claim down
and has to revisit it when it breaks.

## Stages

The stage named is the **last one that succeeded**.

| Stage | Meaning |
|---|---|
| `reject` | the parser refuses it |
| `parse` | parses, but the analyzer reports errors |
| `analyze` | analyzes clean, but the binder cannot build a plan |
| `optimize` | binds and optimizes, but the physical planner cannot lower it |
| `full` | lowers to a physical plan |

`full` is the only stage that needs no cause. **Anything short of `full` is a
limit, and a limit without a stated cause is what this document exists to
prevent** — the runner fails a row that stops early and says nothing about why.

Catalog for the SQL below: `users(id, name, age, city)`,
`orders(id, user_id, amount)`, `emp(id, mgr_id, dept_id, salary)`.

## The surface

| Construct | SQL | Stage | Cause |
|---|---|---|---|
| SELECT with WHERE | `SELECT name FROM users WHERE age > 30` | full | |
| INNER JOIN | `SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id` | full | |
| LEFT JOIN | `SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id` | full | |
| self-join (aliased) | `SELECT a.name FROM users a JOIN users b ON a.id = b.id` | full | |
| JOIN … USING | `SELECT u.id FROM users u JOIN orders o USING (id)` | full | |
| NATURAL JOIN | `SELECT u.id FROM users u NATURAL JOIN orders o` | full | |
| CROSS JOIN | `SELECT u.name FROM users u CROSS JOIN orders o` | full | |
| LATERAL join | `SELECT u.name FROM users u, LATERAL (SELECT o.amount FROM orders o WHERE o.user_id = u.id) s` | full | |
| GROUP BY + aggregate | `SELECT city, COUNT(*) FROM users GROUP BY city` | full | |
| HAVING over an aggregate | `SELECT city, SUM(age) FROM users GROUP BY city HAVING SUM(age) > 10` | full | |
| GROUPING SETS | `SELECT city, age, COUNT(*) FROM users GROUP BY GROUPING SETS ((city), (age))` | full | |
| ROLLUP | `SELECT city, age, COUNT(*) FROM users GROUP BY ROLLUP (city, age)` | full | |
| window function | `SELECT name, RANK() OVER (PARTITION BY city ORDER BY age) FROM users` | full | |
| DISTINCT | `SELECT DISTINCT city FROM users` | full | |
| ORDER BY + LIMIT | `SELECT name FROM users ORDER BY age LIMIT 5` | full | |
| CASE and CAST | `SELECT CASE WHEN age > 30 THEN 1 ELSE 0 END, CAST(age AS BIGINT) FROM users` | full | |
| scalar subquery | `SELECT (SELECT COUNT(*) FROM orders) FROM users` | full | |
| correlated EXISTS | `SELECT name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)` | full | |
| IN subquery | `SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)` | full | |
| UNION | `SELECT id FROM users UNION SELECT user_id FROM orders` | full | |
| UNION ALL | `SELECT id FROM users UNION ALL SELECT user_id FROM orders` | full | |
| EXCEPT | `SELECT id FROM users EXCEPT SELECT user_id FROM orders` | full | |
| CTE | `WITH c AS (SELECT id FROM users) SELECT id FROM c` | full | |
| recursive CTE | `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r` | full | |
| standalone VALUES | `VALUES (1, 2), (3, 4)` | full | |
| INSERT | `INSERT INTO users (id, name) VALUES (1, 'a')` | full | |
| INSERT … RETURNING | `INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id` | full | |
| UPDATE | `UPDATE users SET age = 1 WHERE id = 2` | full | |
| DELETE | `DELETE FROM users WHERE id = 2` | full | |
| CREATE TABLE AS | `CREATE TABLE t2 AS SELECT id FROM users` | full | |
| GROUP BY ordinal | `SELECT city, COUNT(*) FROM users GROUP BY 1` | full | |
| unknown column | `SELECT nosuchcol FROM users` | parse | The analyzer is the authority on names; an unresolved column is a diagnostic, not a plan. |
| unknown table | `SELECT id FROM nosuchtable` | parse | As above, for relations. |
| aggregate in WHERE | `SELECT name FROM users WHERE COUNT(*) > 1` | parse | An aggregate cannot be evaluated before grouping; SQL puts this in HAVING. |
