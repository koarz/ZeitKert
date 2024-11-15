# Create
1. Create Database
2. Create Table With ColumnName And Type (Supported type : int, double, string)
```sql
create database test;
create table test(id int, name string, score double);
```
# Show
```sql
show databases;
show tables;
```
# Use
```sql
use test;
```

# Select
1. Select Constant, It Will Create Some Columns (After Support ColumnConst Would Store Column Into ColumnConst)
2. Select Function, Support Function Nesting
```sql
select 1, 2.0, '3';
select function(function(arg...));
```

# Insert
1. Insert Into Table With Tuples
```sql
insert into table values(tuple...);
```