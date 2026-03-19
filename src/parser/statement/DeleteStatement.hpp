#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/EnumClass.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/binder/BoundExpress.hpp"

namespace DB {
struct DeleteStatement : public SQLStatement {

  DeleteStatement() : SQLStatement(StatementType::DeleteStatement) {}

  ~DeleteStatement() override = default;

  TableMetaRef table_;

  BoundExpressRef where_condition_;
};
} // namespace DB
