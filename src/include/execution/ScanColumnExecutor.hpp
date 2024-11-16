#pragma once

#include "catalog/meta/ColumnMeta.hpp"
#include "execution/AbstractExecutor.hpp"

namespace DB {
class ScanColumnExecutor : public AbstractExecutor {
  ColumnMetaRef column_meta_;

public:
  ScanColumnExecutor(SchemaRef schema, ColumnMetaRef column_meta)
      : AbstractExecutor(schema), column_meta_(column_meta) {}

  ~ScanColumnExecutor() override = default;

  Status Init() override;

  Status Execute() override;
};
} // namespace DB