#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "parser/binder/BoundExpress.hpp"

namespace DB {
class BoundColumnMeta : public BoundExpress {
  TableMetaRef table_meta_;
  ColumnMetaRef column_meta_;

public:
  BoundColumnMeta(TableMetaRef table_meta, ColumnMetaRef column_meta)
      : BoundExpress(BoundExpressType::BoundColumnMeta),
        table_meta_(std::move(table_meta)),
        column_meta_(std::move(column_meta)) {}
  ~BoundColumnMeta() override = default;

  ColumnMetaRef GetColumnMeta() { return column_meta_; }

  TableMetaRef GetTableMeta() { return table_meta_; }
};
} // namespace DB
