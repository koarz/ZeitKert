#pragma once

#include "common/EnumClass.hpp"
#include "parser/SQLStatement.hpp"

#include <string>

namespace DB {
class DropStatement : public SQLStatement {
  DropType type_;
  std::string name_;

public:
  DropStatement(DropType type, std::string name)
      : SQLStatement(StatementType::DROP_STATEMENT), type_(type),
        name_(std::move(name)) {}
  ~DropStatement() override = default;

  DropType GetType() { return type_; }

  std::string &GetName() { return name_; }
};
} // namespace DB