CREATE TABLE emp    (id INTEGER, name VARCHAR, dept_id INTEGER, salary INTEGER);
CREATE TABLE dept   (id INTEGER, name VARCHAR);
CREATE TABLE orders (id INTEGER, user_id INTEGER, total INTEGER);
CREATE TABLE cust   (id INTEGER, name VARCHAR, city VARCHAR);
CREATE TABLE prod   (id INTEGER, name VARCHAR, price INTEGER);
CREATE TABLE items  (id INTEGER, order_id INTEGER, prod_id INTEGER, qty INTEGER);
