#pragma once
enum class CreateType {
  Database,
  Table,
};

enum class ErrorCode {
  OK,
  CreateError,
  DatabaseNotExists,
  DropError,
  SyntaxError,
  BindError,
  NotChoiceDatabase,
  IOError,
  BufferPoolError,
  NotFound,
  DataTooLarge,
  InsertError,
  FileNotOpen,
};

enum class StatementType {
  InvalidStatement,
  CreateStatement,
  UseStatement,
  ShowStatement,
  SelectStatement,
  InsertStatement,
  DropStatement,
  FlushStatement,
};

enum class ASTNodeType {
  InValid,
  CreateQuery,
  UseQuery,
  ShowQuery,
  DropQuery,
  SelectQuery,
  ColumnRef,
  TableRef,
  Function,
  InsertQuery,
  TableNames,
  Token,
  FlushQuery,
};

enum class ShowType {
  Databases,
  Tables,
};

enum class BoundExpressType {
  BoundConstant,
  BoundFunction,
  BoundTuple,
  BoundColumnMeta,
};

enum class PlanType {
  SeqScan,
  IndexScan,
  Insert,
  Update,
  Delete,
  Aggregation,
  Limit,
  NestedLoopJoin,
  NestedIndexJoin,
  HashJoin,
  Filter,
  Values,
  Function,
  Tuple,
  Projection,
  Sort,
  TopN,
  MockScan,
  InitCheck
};

enum class DropType {
  Table,
  Database,
};