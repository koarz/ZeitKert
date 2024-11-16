#pragma once

#include "catalog/meta/ColumnMeta.hpp"
#include "parser/binder/BoundExpress.hpp"

namespace DB {
class BoundColumnMeta : public BoundExpress {
  ColumnMetaRef column_meta_;

public:
  BoundColumnMeta(ColumnMetaRef column_meta)
      : BoundExpress(BoundExpressType::BoundColumnMeta),
        column_meta_(column_meta) {}
  ~BoundColumnMeta() override = default;

  ColumnMetaRef GetColumnMeta() { return column_meta_; }
};
} // namespace DB