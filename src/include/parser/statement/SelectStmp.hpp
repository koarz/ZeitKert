#pragma once

#include "parser/SQLStatement.hpp"

namespace DB {
class SelectStmp : public SQLStatement {
public:
  static constexpr const StatementType TYPE = StatementType::SELECT_STATEMENT;

  SelectStmp() : SQLStatement(StatementType::SELECT_STATEMENT) {}
};
} // namespace DB