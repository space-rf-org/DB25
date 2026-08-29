-- DB25 curated session (authored, not harvested) exercising the frontend gaps
-- closed as query/plan features: G4 qualified star over a join, G2 quantified
-- comparison (ALL / ANY / SOME), and G3 CTAS (CREATE TABLE AS <query>), whose
-- derived table is registered so a later statement can query it.
CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE emp (id INTEGER NOT NULL, name TEXT, dept_id INTEGER, salary DOUBLE);

-- G4: a qualified star over a join projects exactly that relation's columns
SELECT e.* FROM emp e JOIN dept d ON e.dept_id = d.id;
-- G2: quantified comparison against a subquery (ALL / ANY / SOME)
SELECT id FROM emp WHERE salary > ALL (SELECT salary FROM emp);
SELECT id FROM emp WHERE dept_id = ANY (SELECT id FROM dept);
SELECT id FROM emp WHERE salary < SOME (SELECT salary FROM emp WHERE dept_id = 1);
-- G3: CTAS registers the derived table (columns from the query's projection)
CREATE TABLE high_earners AS SELECT id, salary FROM emp WHERE salary > 100;
CREATE TABLE emp_dept AS SELECT e.id, d.name FROM emp e JOIN dept d ON e.dept_id = d.id;
-- the CTAS tables are now queryable: these resolve against the registered schema
SELECT id, salary FROM high_earners;
SELECT name FROM emp_dept;
