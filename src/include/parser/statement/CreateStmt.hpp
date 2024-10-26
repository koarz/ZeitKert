#pragma once

#include "parser/SQLStatement.hpp"

namespace DB {
class CreateStmt : public SQLStatement {

public:
  enum class CreateType { TABLE };
  static constexpr const StatementType TYPE = StatementType::INVALID_STATEMENT;

  CreateStmt() : SQLStatement(StatementType::CREATE_STATEMENT) {}
  ~CreateStmt() override = default;

private:
  std::string query_;

  CreateType type_;
};
} // namespace DB