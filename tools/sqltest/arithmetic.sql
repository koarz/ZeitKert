# 算术运算符测试 (+ - * / %)

statement ok
CREATE DATABASE arith_test_db

statement ok
USE arith_test_db

statement ok
CREATE TABLE nums (id INT, a INT, b INT, x DOUBLE) UNIQUE KEY (id)

statement ok
INSERT INTO nums VALUES (1, 10, 3, 2.5)

statement ok
INSERT INTO nums VALUES (2, 7, 2, 3.0)

statement ok
INSERT INTO nums VALUES (3, 0, 5, 0.0)

statement ok
INSERT INTO nums VALUES (4, 15, 4, 7.5)

# ---- 基本算术运算 ----
query
SELECT a + b FROM nums WHERE id = 1
----
13

query
SELECT a - b FROM nums WHERE id = 1
----
7

query
SELECT a * b FROM nums WHERE id = 1
----
30

query
SELECT a / b FROM nums WHERE id = 1
----
3

# ---- 取模运算 (INT % INT) ----
query
SELECT a % b FROM nums WHERE id = 1
----
1

query
SELECT a % b FROM nums WHERE id = 2
----
1

query
SELECT a % b FROM nums WHERE id = 3
----
0

query
SELECT a % b FROM nums WHERE id = 4
----
3

# ---- 取模运算 (DOUBLE) ----
query
SELECT x % b FROM nums WHERE id = 1
----
2.500000

query
SELECT a % x FROM nums WHERE id = 2
----
1.000000

# ---- 取模除零 ----
statement error
SELECT a % x FROM nums WHERE id = 3

# ---- 运算符优先级: % 与 * / 同级，高于 + - ----
query
SELECT a + b % 2 FROM nums WHERE id = 4
----
15

query
SELECT a - b % 3 FROM nums WHERE id = 4
----
14

# ---- 组合表达式 ----
query
SELECT a % b + a / b FROM nums WHERE id = 1
----
4

query
SELECT (a + b) % 4 FROM nums WHERE id = 1
----
1

# 清理
statement ok
DROP TABLE nums

statement ok
DROP DATABASE arith_test_db
