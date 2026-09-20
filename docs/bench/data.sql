TRUNCATE emp, dept, orders, cust, prod, items;
INSERT INTO emp    SELECT g, 'e'||g, g%50,  (g*37)%20000 FROM generate_series(1,20000) g;
INSERT INTO dept   SELECT g, 'd'||g FROM generate_series(1,50) g;
INSERT INTO orders SELECT g, g%20000, (g*13)%5000 FROM generate_series(1,50000) g;
INSERT INTO cust   SELECT g, 'c'||g, 'city'||(g%100) FROM generate_series(1,20000) g;
INSERT INTO prod   SELECT g, 'p'||g, (g*7)%1000 FROM generate_series(1,2000) g;
INSERT INTO items  SELECT g, g%50000, g%2000, g%10 FROM generate_series(1,100000) g;
ANALYZE;
