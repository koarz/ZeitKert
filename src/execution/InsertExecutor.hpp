#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "storage/lsmtree/LSMTree.hpp"

namespace DB {

// 写入算子（向量化版本）
//
// 当前 Insert 是终结态算子：在 Init 阶段完成所有真正的写入，
// Next 一次性发射 "InsertRows: N" 结果列后立即 EOF
//
// 子节点（VALUES/Tuple）的产出完全在 Init 内部消耗，不再走 schema_ 共享通路
class InsertExecutor : public AbstractExecutor {
  TableMetaRef table_meta_;
  std::vector<AbstractExecutorRef> children_;
  std::shared_ptr<LSMTree> lsm_tree_;
  size_t bulk_rows_{0};

  // 写入完成后缓存的行数，Next 时用于构造结果列
  uint32_t inserted_rows_{0};
  bool emitted_{false};

public:
  InsertExecutor(SchemaRef schema, std::vector<AbstractExecutorRef> children,
                 TableMetaRef table_meta, std::shared_ptr<LSMTree> lsm_tree,
                 size_t bulk_rows = 0)
      : AbstractExecutor(schema), children_(std::move(children)),
        table_meta_(std::move(table_meta)), lsm_tree_(std::move(lsm_tree)),
        bulk_rows_(bulk_rows) {}

  ~InsertExecutor() override = default;

  // TODO:
  // insert may suffer concurrency error
  // when some insert executor insert to one column at same time
  // so we need version control to save newest data
  Status Init() override;
  Status Next(DataChunk &out) override;
};
} // namespace DB
