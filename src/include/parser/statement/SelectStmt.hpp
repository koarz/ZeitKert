#pragma once

#include "parser/SQLStatement.hpp"
#include "parser/binder/BoundExpress.hpp"
#include <memory>

namespace DB {
struct SelectStmt : public SQLStatement {

  SelectStmt() : SQLStatement(StatementType::SELECT_STATEMENT) {}

  ~SelectStmt() override = default;

  std::vector<BoundExpressRef> columns_;

  std::vector<BoundExpressRef> from_;
};
} // namespace DB