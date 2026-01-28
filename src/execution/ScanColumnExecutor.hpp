#pragma once

#include "catalog/meta/ColumnMeta.hpp"
#include "execution/AbstractExecutor.hpp"
#include "storage/lsmtree/LSMTree.hpp"

namespace DB {
class ScanColumnExecutor : public AbstractExecutor {
  ColumnMetaRef column_meta_;
  std::shared_ptr<LSMTree> lsm_tree_;
  uint32_t column_idx_{0};

public:
  ScanColumnExecutor(SchemaRef schema, ColumnMetaRef column_meta,
                     std::shared_ptr<LSMTree> lsm_tree, uint32_t column_idx)
      : AbstractExecutor(schema), column_meta_(std::move(column_meta)),
        lsm_tree_(std::move(lsm_tree)), column_idx_(column_idx) {}

  ~ScanColumnExecutor() override = default;

  Status Execute() override;
};
} // namespace DB
