# Math functions 测试

statement ok
CREATE DATABASE math_test_db

statement ok
USE math_test_db

statement ok
CREATE TABLE nums (id INT, val DOUBLE) UNIQUE KEY (id)

# 插入测试数据
statement ok
INSERT INTO nums VALUES (1, 0.0)

statement ok
INSERT INTO nums VALUES (2, 1.0)

statement ok
INSERT INTO nums VALUES (3, 4.0)

statement ok
INSERT INTO nums VALUES (4, 3.7)

statement ok
INSERT INTO nums VALUES (5, 100.0)

# ---- SQRT ----
query
SELECT SQRT(val) FROM nums WHERE id = 3
----
2.000000

query
SELECT SQRT(val) FROM nums WHERE id = 2
----
1.000000

query
SELECT SQRT(val) FROM nums WHERE id = 1
----
0.000000

# ---- SIN / COS ----
query
SELECT SIN(val) FROM nums WHERE id = 1
----
0.000000

query
SELECT COS(val) FROM nums WHERE id = 1
----
1.000000

query
SELECT SIN(val), COS(val) FROM nums WHERE id = 2
----
0.841471 0.540302

# ---- TAN ----
query
SELECT TAN(val) FROM nums WHERE id = 1
----
0.000000

query
SELECT TAN(val) FROM nums WHERE id = 2
----
1.557408

# ---- ASIN / ACOS / ATAN ----
query
SELECT ASIN(val) FROM nums WHERE id = 2
----
1.570796

query
SELECT ACOS(val) FROM nums WHERE id = 2
----
0.000000

query
SELECT ATAN(val) FROM nums WHERE id = 2
----
0.785398

# ---- LOG / LOG10 ----
query
SELECT LOG(val) FROM nums WHERE id = 2
----
0.000000

query
SELECT LOG10(val) FROM nums WHERE id = 5
----
2.000000

# ---- EXP ----
query
SELECT EXP(val) FROM nums WHERE id = 1
----
1.000000

query
SELECT EXP(val) FROM nums WHERE id = 2
----
2.718282

# ---- CEIL / FLOOR / ROUND ----
query
SELECT CEIL(val) FROM nums WHERE id = 4
----
4.000000

query
SELECT FLOOR(val) FROM nums WHERE id = 4
----
3.000000

query
SELECT ROUND(val) FROM nums WHERE id = 4
----
4.000000

# 多个math函数组合查询
query
SELECT CEIL(val), FLOOR(val), ROUND(val) FROM nums WHERE id = 4
----
4.000000 3.000000 4.000000

# 对INT列使用math函数
query
SELECT SQRT(id) FROM nums WHERE id = 4
----
2.000000

query
SELECT SIN(id), COS(id) FROM nums WHERE id = 1
----
0.841471 0.540302

# 清理
statement ok
DROP TABLE nums

statement ok
DROP DATABASE math_test_db
