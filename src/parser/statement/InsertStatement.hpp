#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/EnumClass.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/statement/SelectStatement.hpp"

#include <memory>

namespace DB {
struct InsertStatement : public SQLStatement {

  InsertStatement() : SQLStatement(StatementType::InsertStatement) {}

  ~InsertStatement() override = default;

  std::vector<BoundExpressRef> tuples_;

  std::shared_ptr<SelectStatement> select_;

  TableMetaRef table_;

  void SetBulkRows(size_t rows) {
    is_bulk_ = true;
    bulk_rows_ = rows;
  }

  bool IsBulk() const { return is_bulk_; }

  size_t GetBulkRows() const { return bulk_rows_; }

private:
  bool is_bulk_{false};
  size_t bulk_rows_{0};
};
} // namespace DB
