#pragma once
enum class CreateType {
  DATABASE,
  TABLE,
};

enum class ErrorCode {
  OK,
  CreateError,
  DatabaseNotExists,
  DropError,
  SyntaxError,
  BindError,
  NotChoiceDatabase,
};

enum class StatementType {
  INVALID_STATEMENT,
  CREATE_STATEMENT,
  USE_STATEMENT,
  SHOW_STATEMENT,
  SELECT_STATEMENT,
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
  Token,
};

enum class ShowType {
  Databases,
  Tables,
};

enum class BoundExpressType {
  BoundConstant,
  BoundFunction,
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
  Projection,
  Sort,
  TopN,
  MockScan,
  InitCheck
};