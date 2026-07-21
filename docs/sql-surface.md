# DB25 SQL Surface (stack-tested)

What SQL the **whole stack** accepts — tokenizer → parser → analyzer → binder →
optimizer — as opposed to what the parser alone accepts. The distinction matters: the
parser is deliberately permissive, so **parser-accepts ≠ stack-accepts**.

## The contract

- The formal accepted-SQL grammar is the parser's
  [`grammar/DB25_SQL_GRAMMAR.ebnf`](https://github.com/space-rf-org/db25-sql-parser/blob/main/grammar/DB25_SQL_GRAMMAR.ebnf)
  and the runnable corpus
  [`ebnf_supported.sql`](https://github.com/space-rf-org/db25-sql-parser/blob/main/ebnf_supported.sql)
  (referenced, not forked, so they can't drift). Relocation source: `db25-sql-parser@ee2ec99`.
- The parser's own `docs/SQL_SUPPORT.md` describes the **parser** surface. This page
  reconciles it against the **stack**. Note: the parser repo's `EBNF_COVERAGE_REPORT.md`
  is stale and self-contradictory (it claims transaction control unimplemented while the
  grammar and SQL_SUPPORT list it) — do not treat its numbers as authoritative.

## Method

- **Parser surface:** the `ebnf_supported.sql` corpus parses **24 / 25** statements.
- **Stack surface:** each construct below was run through all five stages over a fixed
  catalog (`users`, `orders`, `emp`), recording the furthest stage reached. Reproduce
  with a driver that calls `parse → analyze → bind → optimize` (the same pipeline the
  harness uses).

## Stack support (tested)

| Construct | Furthest stage | Notes |
|---|---|---|
| `SELECT … WHERE` | **full** | |
| `INNER JOIN`, `LEFT JOIN` | **full** | |
| self-join (aliased) | **full** | correct since `db25-logical-plan#23` |
| `GROUP BY` + aggregates | **full** | |
| `HAVING` over an aggregate | **full** | e.g. `HAVING SUM(x) > k` (since `#22`) |
| window function `… OVER (…)` | **full** | |
| `DISTINCT` | **full** | |
| `ORDER BY` … `LIMIT`/`OFFSET` | **full** | |
| `CASE`, `CAST` | **full** | |
| scalar / `IN` / correlated `EXISTS` subqueries | **full** | decorrelated to semi/anti/left joins |
| `UNION` (set ops) | **full** | |
| `INSERT` / `UPDATE` / `DELETE` | **full** | |
| `NATURAL JOIN` | **full** but **incorrect** | binds/optimizes yet drops the right relation — **broken semantics**, tracked (`db25-logical-plan#4`) |
| `JOIN … USING (col)` | **parse-only** | parser accepts; analyzer/binder reject or mis-lower (`#4`) |
| `GROUP BY <ordinal>` (`GROUP BY 1`) | **parse-only** | analyzer rejects ordinal/alias grouping (by design, for now) |
| `VALUES (…), (…)` standalone | **analyze-only** | binder does not yet lower a standalone `VALUES` |
| `WITH … (CTE)` | **analyze-only** | binder does not yet lower CTEs |

`full` = tokenizes, parses, analyzes cleanly, binds, and optimizes. Anything short of
`full` is a stack gap even though the parser accepts it.

## Reading this honestly

- **Accepted ≠ correct.** `NATURAL JOIN` reaches `full` but produces wrong results — a
  reminder that "the stack ran it" is not "the stack is right about it." Correctness is
  the harness's job (`docs/harness-findings.md`), not this surface map.
- The gaps here (`USING`, standalone `VALUES`, CTE lowering, `GROUP BY` ordinal) are the
  honest edge of stack support today; update this table as the binder grows.
