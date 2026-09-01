-- DB25 REFERENCE QUERY
--
-- This is the query DB25 is measured against. It is the query used in the
-- position paper's comparison with PostgreSQL and DuckDB, and it stays the
-- reference from here on: when someone asks "how fast is DB25", this is the
-- query that answers.
--
-- It was chosen because it is not a microbenchmark. It exercises a CTE with
-- aggregation, an inner join against that CTE, a window function with its own
-- PARTITION BY / ORDER BY, a correlated LATERAL subquery with its own
-- aggregate, a predicate that compares a column against a CTE-computed value,
-- and an ORDER BY over a window output under a LIMIT. A frontend that is fast
-- on this is fast on real analytics SQL.
--
-- Schema (create this verbatim in any engine you compare against):
--
--   CREATE TABLE emp    (id INTEGER NOT NULL, name VARCHAR,
--                        dept_id INTEGER, salary DOUBLE PRECISION);
--   CREATE TABLE orders (id INTEGER NOT NULL, user_id INTEGER,
--                        total DOUBLE PRECISION);
--
-- Comparison method: empty tables, no statistics, warm medians, release build.
-- PostgreSQL is measured with EXPLAIN (SUMMARY) "Planning Time", which excludes
-- raw parse and parse analysis; DB25's parse->physical number includes them, so
-- the comparison is if anything unkind to DB25 at the front and kind to it at
-- the back.
--
-- Everything below this line is the query. Comment lines are stripped by the
-- reader; do not put SQL after a `--` on the same line.

WITH dept_stats AS (
  SELECT dept_id, AVG(salary) AS avg_sal, COUNT(*) AS headcount
  FROM emp GROUP BY dept_id)
SELECT e.name, e.salary, ds.avg_sal,
       RANK() OVER (PARTITION BY e.dept_id ORDER BY e.salary DESC) AS rnk,
       ord.total
FROM emp e
JOIN dept_stats ds ON e.dept_id = ds.dept_id
LEFT JOIN LATERAL (
  SELECT SUM(o.total) AS total FROM orders o WHERE o.user_id = e.id
) ord ON true
WHERE e.salary > ds.avg_sal
ORDER BY rnk LIMIT 10;
