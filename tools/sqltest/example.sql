# 创建数据库测试
statement ok
CREATE DATABASE test_db

statement ok
USE test_db

# 创建表测试
statement ok
CREATE TABLE users (uid INT, name STRING) UNIQUE KEY (uid)

# 插入数据测试
statement ok
INSERT INTO users VALUES (1, 'Alice')

statement ok
INSERT INTO users VALUES (2, 'Bob')

statement ok
INSERT INTO users VALUES (3, 'Charlie')

# 查询测试
query
SELECT * FROM users
----
1 Alice
2 Bob
3 Charlie

# Count测试
query
SELECT COUNT(uid) FROM users
----
3

# WHERE 基本比较测试
query
SELECT * FROM users WHERE uid = 1
----
1 Alice

query
SELECT * FROM users WHERE uid > 1
----
2 Bob
3 Charlie

query
SELECT * FROM users WHERE uid >= 2
----
2 Bob
3 Charlie

query
SELECT * FROM users WHERE uid < 3
----
1 Alice
2 Bob

query
SELECT * FROM users WHERE uid != 2
----
1 Alice
3 Charlie

# WHERE 字符串比较测试
query
SELECT * FROM users WHERE name = 'Bob'
----
2 Bob

# WHERE AND 逻辑组合测试
query
SELECT * FROM users WHERE uid > 1 AND uid < 3
----
2 Bob

# WHERE OR 逻辑组合测试
query
SELECT * FROM users WHERE uid = 1 OR uid = 3
----
1 Alice
3 Charlie

# WHERE 复合逻辑测试
query
SELECT * FROM users WHERE uid = 1 OR uid = 2 AND name = 'Bob'
----
1 Alice
2 Bob

# WHERE 聚合函数测试
query
SELECT COUNT(uid) FROM users WHERE uid > 1
----
2

# Unique key测试 - 插入重复uid应该更新
statement ok
INSERT INTO users VALUES (1, 'Alice Updated')

query
SELECT * FROM users
----
1 Alice Updated
2 Bob
3 Charlie

# 删除表测试
statement ok
DROP TABLE users

# 删除数据库测试
statement ok
DROP DATABASE test_db

# 错误测试 - 使用不存在的数据库
statement error
USE nonexistent_db

# 错误测试 - 在没有选择数据库时创建表
statement error
CREATE TABLE test (id INT)
