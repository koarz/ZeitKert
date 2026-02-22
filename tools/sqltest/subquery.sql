# 子查询测试

# 基本常量子查询
query
SELECT * FROM (SELECT 1)
----
1

# 多列常量子查询
query
SELECT * FROM (SELECT 1, 2)
----
1 2

# 两层嵌套子查询
query
SELECT * FROM (SELECT * FROM (SELECT 1))
----
1

# 三层嵌套子查询
query
SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT 1)))
----
1

# 子查询包含 range 表函数
query
SELECT * FROM (SELECT * FROM range(1, 4))
----
1
2
3

# 子查询嵌套 range
query
SELECT * FROM (SELECT * FROM (SELECT * FROM range(1, 4)))
----
1
2
3

# 子查询内使用聚合函数
query
SELECT * FROM (SELECT COUNT(range) FROM range(1, 6))
----
5

# 子查询内多列常量（含负数）
query
SELECT * FROM (SELECT -1, 2)
----
-1 2

# 带数据库的子查询测试
statement ok
CREATE DATABASE subquery_test_db

statement ok
USE subquery_test_db

statement ok
CREATE TABLE t (id INT, val INT) UNIQUE KEY (id)

statement ok
INSERT INTO t VALUES (1, 10)

statement ok
INSERT INTO t VALUES (2, 20)

statement ok
INSERT INTO t VALUES (3, 30)

# 对真实表执行子查询
query
SELECT * FROM (SELECT COUNT(id) FROM t)
----
3

# 清理
statement ok
DROP TABLE t

statement ok
DROP DATABASE subquery_test_db
