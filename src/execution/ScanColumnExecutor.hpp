#pragma once

#include "catalog/meta/ColumnMeta.hpp"
#include "execution/AbstractExecutor.hpp"
#include "storage/lsmtree/LSMTree.hpp"

namespace DB {

// 列扫描算子（向量化版本）
//
// Init 阶段：从 LSMTree（或 FilteredDataCache）一次性读出整列数据，保留
// ColumnVector 的 DataSpan 零拷贝引用，避免 mmap 拷贝
// Next 阶段：将整列以单个 DataChunk 发射；后续调用返回空 chunk 表示 EOF
//   （保持单 chunk 发射是为了不破坏 zero-copy span，参见项目优化备忘录）
class ScanColumnExecutor : public AbstractExecutor {
  ColumnMetaRef column_meta_;
  std::shared_ptr<LSMTree> lsm_tree_;
  uint32_t column_idx_{0};

  // 缓存的整列数据（Init 阶段写入，Next 阶段消费）
  ColumnPtr column_;
  // 是否已经发射过一次完整数据
  bool emitted_{false};

public:
  ScanColumnExecutor(SchemaRef schema, ColumnMetaRef column_meta,
                     std::shared_ptr<LSMTree> lsm_tree, uint32_t column_idx)
      : AbstractExecutor(schema), column_meta_(std::move(column_meta)),
        lsm_tree_(std::move(lsm_tree)), column_idx_(column_idx) {}

  ~ScanColumnExecutor() override = default;

  Status Init() override;
  Status Next(DataChunk &out) override;
};

} // namespace DB
