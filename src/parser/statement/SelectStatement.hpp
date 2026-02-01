#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/EnumClass.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/binder/BoundExpress.hpp"

namespace DB {
struct SelectStatement : public SQLStatement {

  SelectStatement() : SQLStatement(StatementType::SelectStatement) {}

  ~SelectStatement() override = default;

  std::vector<BoundExpressRef> columns_;

  std::vector<TableMetaRef> from_;

  // WHERE 条件表达式，为 nullptr 表示没有 WHERE 子句
  BoundExpressRef where_condition_;
};
} // namespace DB