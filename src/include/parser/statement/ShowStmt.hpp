#pragma once

#include "common/EnumClass.hpp"
#include "parser/SQLStatement.hpp"

namespace DB {
class ShowStmt : public SQLStatement {
  ShowType show_type_;

public:
  explicit ShowStmt(ShowType show_type)
      : SQLStatement(StatementType::SHOW_STATEMENT), show_type_(show_type) {}

  ~ShowStmt() override = default;

  ShowType GetShowType() { return show_type_; }
};
} // namespace DB