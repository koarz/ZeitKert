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
  Token,
};

enum class ShowType {
  Databases,
  Tables,
};