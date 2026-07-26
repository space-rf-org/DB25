-- DDL: create the parent table with a primary key and a unique constraint
CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT NOT NULL, CONSTRAINT uq_name UNIQUE (name));
-- DDL: create a child table with a NOT NULL column, a DEFAULT, a CHECK and a FK
CREATE TABLE emp (id INTEGER NOT NULL, name TEXT, dept_id INTEGER, salary DOUBLE DEFAULT 0, CONSTRAINT ck_sal CHECK (salary >= 0), CONSTRAINT fk_dept FOREIGN KEY (dept_id) REFERENCES dept (id));
-- DDL: secondary index
CREATE INDEX ix_emp_dept ON emp (dept_id);
-- DDL: ALTER TABLE add a column with a default
ALTER TABLE emp ADD COLUMN active INTEGER DEFAULT 1;
-- DML: a clean multi-row INSERT
INSERT INTO dept (id, name) VALUES (1, 'eng'), (2, 'sales');
-- DML: INSERT that violates the CHECK constraint (salary = -5)
INSERT INTO emp (id, name, dept_id, salary) VALUES (1, 'ann', 1, -5);
-- DML: UPDATE with a WHERE predicate
UPDATE emp SET salary = salary * 1.1 WHERE dept_id = 1;
-- DML: DELETE with a predicate
DELETE FROM emp WHERE active = 0;
-- Query: simple projection with a filter
SELECT id, name FROM emp WHERE salary > 100;
-- Query: inner join across the FK
SELECT e.name, d.name FROM emp e JOIN dept d ON e.dept_id = d.id;
-- Query: aggregation with GROUP BY and HAVING
SELECT dept_id, COUNT(*), AVG(salary) FROM emp GROUP BY dept_id HAVING COUNT(*) > 1;
-- Query: scalar subquery in the WHERE clause
SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp);
-- Query: correlated EXISTS subquery
SELECT d.name FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id);
-- Query: IN subquery
SELECT name FROM emp WHERE dept_id IN (SELECT id FROM dept WHERE name = 'eng');
-- Query: common table expression (CTE)
WITH big AS (SELECT dept_id FROM emp WHERE salary > 100) SELECT dept_id FROM big;
-- Query: window function
SELECT name, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) FROM emp;
-- Query: set operation (UNION)
SELECT id FROM dept UNION SELECT dept_id FROM emp;
-- Query: set operation (INTERSECT)
SELECT id FROM dept INTERSECT SELECT dept_id FROM emp;
-- Query: CASE expression
SELECT name, CASE WHEN salary > 100 THEN 'high' ELSE 'low' END FROM emp;
-- Query: LEFT JOIN (outer join nullability shows in the plan schema)
SELECT d.name, e.name FROM dept d LEFT JOIN emp e ON e.dept_id = d.id;
-- Query: self-join (same table twice under different aliases)
SELECT a.name, b.name FROM emp a JOIN emp b ON a.dept_id = b.dept_id AND a.id <> b.id;
-- Query: derived table column-alias list — end to end: the alias list renames the derived output columns, and s.hi (an aliased COMPUTED column, MAX) resolves and plans through the rename
SELECT s.hi FROM (SELECT dept_id, MAX(salary) FROM emp GROUP BY dept_id) AS s(dept, hi);
-- Query: VALUES derived table — end to end: (VALUES ...) AS v(id, label) lowers to a Values node whose columns are named by the alias list and typed, so v.id / v.label resolve and plan
SELECT v.label FROM (VALUES (1, 'eng'), (2, 'sales')) AS v(id, label) WHERE v.id = 1;
-- Query: EXCEPT set operation
SELECT id FROM dept EXCEPT SELECT dept_id FROM emp;
-- Query: DISTINCT projection
SELECT DISTINCT dept_id FROM emp;
-- Query: ORDER BY with LIMIT and OFFSET
SELECT name, salary FROM emp ORDER BY salary DESC LIMIT 5 OFFSET 2;
-- Query: BETWEEN, IN-list and LIKE predicates together
SELECT name FROM emp WHERE salary BETWEEN 50 AND 500 AND dept_id IN (1, 2) AND name LIKE 'a%';
-- Query: CAST and COALESCE / NULLIF scalar functions
SELECT CAST(salary AS INTEGER), COALESCE(name, 'n/a'), NULLIF(dept_id, 0) FROM emp;
-- Query: two chained CTEs, the second referencing the first
WITH per_dept AS (SELECT dept_id, COUNT(*) AS n FROM emp GROUP BY dept_id), busy AS (SELECT dept_id FROM per_dept WHERE n > 1) SELECT dept_id FROM busy;
-- Query: INSERT ... SELECT (row source is a query, arity checked against the target)
INSERT INTO dept (id, name) SELECT dept_id, 'dup' FROM emp WHERE dept_id IS NOT NULL;
-- DML: UPDATE whose SET value violates the CHECK (salary = -1 vs CHECK salary >= 0)
UPDATE emp SET salary = -1 WHERE id = 1;
-- Query: a deliberate error — an unresolved column, to show the analyze stage catching it
SELECT nonexistent_column FROM emp;
