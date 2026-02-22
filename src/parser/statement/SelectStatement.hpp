#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/EnumClass.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/binder/BoundExpress.hpp"

#include <cstdint>
#include <optional>

namespace DB {

struct RangeInfo {
  int64_t start;
  int64_t stop;
  int64_t step{1};
};

struct SelectStatement : public SQLStatement {

  SelectStatement() : SQLStatement(StatementType::SelectStatement) {}

  ~SelectStatement() override = default;

  std::vector<BoundExpressRef> columns_;

  std::vector<TableMetaRef> from_;

  // WHERE 条件表达式，为 nullptr 表示没有 WHERE 子句
  BoundExpressRef where_condition_;

  std::optional<RangeInfo> range_info_;
  TableMetaRef range_table_;

  std::shared_ptr<SelectStatement> subquery_;
};
} // namespace DB