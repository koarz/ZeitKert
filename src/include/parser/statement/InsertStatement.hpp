#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/binder/BoundExpress.hpp"

namespace DB {
struct InsertStatement : public SQLStatement {

  InsertStatement() : SQLStatement(StatementType::INSERT_STATEMENT) {}

  ~InsertStatement() override = default;

  std::vector<BoundExpressRef> tuples_;

  TableMetaRef table_;
};
} // namespace DB