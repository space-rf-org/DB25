-- DB25 curated LATERAL session (authored, not harvested): every LATERAL join
-- shape the frontend accepts, over a small dept/emp schema. Kept to VALID,
-- accepted forms only - the honest rejections (RIGHT/FULL/NATURAL JOIN LATERAL)
-- live in the parser's negative tests, not here.
CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE emp (id INTEGER NOT NULL, name TEXT, dept_id INTEGER, salary DOUBLE);

-- comma-form LATERAL (a cross join lateral): the derived table is correlated by d.id
SELECT d.name, e.salary FROM dept d, LATERAL (SELECT salary FROM emp WHERE dept_id = d.id) e;
-- CROSS JOIN LATERAL: the explicit spelling of the same shape
SELECT d.name, e.salary FROM dept d CROSS JOIN LATERAL (SELECT salary FROM emp WHERE dept_id = d.id) e;
-- INNER JOIN LATERAL ... ON: an inner lateral join with a join predicate
SELECT d.name, e.salary FROM dept d JOIN LATERAL (SELECT salary FROM emp WHERE dept_id = d.id) e ON e.salary > 0;
-- LEFT JOIN LATERAL ... ON: the RHS is correlated AND null-extended
SELECT d.name, e.salary FROM dept d LEFT JOIN LATERAL (SELECT salary FROM emp WHERE dept_id = d.id) e ON true;
-- LATERAL body referencing an EARLIER comma sibling (d is visible past the second table)
SELECT d.name, e.salary FROM dept d, emp mgr, LATERAL (SELECT salary FROM emp WHERE dept_id = d.id) e;
-- an uncorrelated LATERAL body (no outer reference) is a plain derived table
SELECT d.name, e.salary FROM dept d CROSS JOIN LATERAL (SELECT salary FROM emp WHERE salary > 100) e;
