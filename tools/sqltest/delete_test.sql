# Test DELETE statement

statement ok
CREATE DATABASE test_delete_db

statement ok
USE test_delete_db

statement ok
CREATE TABLE t (id INT, name STRING) UNIQUE KEY (id)

statement ok
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')

query
SELECT * FROM t
----
1 a
2 b
3 c

statement ok
DELETE FROM t WHERE id = 2

query
SELECT * FROM t
----
1 a
3 c

statement ok
DELETE FROM t WHERE id > 1

query
SELECT * FROM t
----
1 a

statement ok
DROP TABLE t

statement ok
DROP DATABASE test_delete_db
