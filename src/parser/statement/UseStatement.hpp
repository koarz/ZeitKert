#pragma once

#include "parser/SQLStatement.hpp"

#include <string>

namespace DB {
class UseStatement : public SQLStatement {
  std::string name_;

public:
  explicit UseStatement(std::string name)
      : SQLStatement(StatementType::UseStatement), name_(std::move(name)) {}

  ~UseStatement() override = default;

  std::string GetName() { return name_; }
};
} // namespace DB