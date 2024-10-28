#pragma once

#include "parser/SQLStatement.hpp"

#include <string>

namespace DB {
class UseStmt : public SQLStatement {
  std::string name_;

public:
  explicit UseStmt(std::string name)
      : SQLStatement(StatementType::USE_STATEMENT), name_(std::move(name)) {}

  ~UseStmt() override = default;

  std::string GetName() { return name_; }
};
} // namespace DB