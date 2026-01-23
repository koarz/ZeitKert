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
