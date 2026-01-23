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
};
} // namespace DB