-- DB25 curated session (authored, not harvested) exercising frontend gaps closed
-- as query/plan features: G4 qualified star over a join, and G2 quantified
-- comparison (ALL / ANY / SOME). (G3 CTAS lowering is covered by the staged
-- fixture 36_create_table_as; it is omitted here because the corpus's DDL path
-- applies statements via execute_ddl, which does not yet register a CTAS-derived
-- table - a separate, catalog-side follow-up from the binder lowering.)
CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE emp (id INTEGER NOT NULL, name TEXT, dept_id INTEGER, salary DOUBLE);

-- G4: a qualified star over a join projects exactly that relation's columns
SELECT e.* FROM emp e JOIN dept d ON e.dept_id = d.id;
-- G2: quantified comparison against a subquery (ALL / ANY / SOME)
SELECT id FROM emp WHERE salary > ALL (SELECT salary FROM emp);
SELECT id FROM emp WHERE dept_id = ANY (SELECT id FROM dept);
SELECT id FROM emp WHERE salary < SOME (SELECT salary FROM emp WHERE dept_id = 1);
