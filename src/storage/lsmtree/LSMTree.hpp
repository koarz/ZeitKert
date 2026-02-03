#pragma once

#include "common/Status.hpp"
#include "storage/IndexEngine.hpp"
#include "storage/column/Column.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/SelectionVector.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/TableOperator.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <map>
#include <memory>
#include <shared_mutex>
#include <vector>

namespace DB {
class LSMTree : public IndexEngine<Slice, Slice, SliceCompare> {
  std::shared_mutex latch_;
  std::shared_mutex immutable_latch_;

  bool write_log_;
  MemTableRef memtable_;
  uint32_t table_number_;
  uint32_t wal_number_{0};
  std::vector<std::shared_ptr<ValueType>> column_types_;
  uint16_t primary_key_idx_{};
  // TODO:
  // the immutable table may reading by other threads
  // so we need safely delete them when immutable was full
  std::vector<MemTableRef> immutable_table_;
  std::map<uint32_t, SSTableRef> sstables_;

  // 主键类型特化的 BuildSelectionVector 实现
  SelectionVector BuildSelectionVectorInt();
  SelectionVector BuildSelectionVectorString();

public:
  LSMTree(std::filesystem::path table_path,
          std::shared_ptr<BufferPoolManager> buffer_pool_manager,
          std::vector<std::shared_ptr<ValueType>> column_types,
          uint16_t primary_key_idx, bool write_log = true);

  ~LSMTree() override;

  Status Insert(const Slice &key, const Slice &value) override;

  Status Remove(const Slice &key) override;

  Status GetValue(const Slice &key, Slice *column) override;

  Status ScanColumn(size_t column_idx, ColumnPtr &res);

  // 构建去重后的 SelectionVector
  SelectionVector BuildSelectionVector();

  size_t GetImmutableSize() { return immutable_table_.size(); }

  uint32_t GetTableNum() { return table_number_; }

  // 手动刷盘：将当前 MemTable 和所有 Immutable tables 刷盘为 SST 文件
  Status FlushToSST();

  uint16_t GetPrimaryKeyIndex() const { return primary_key_idx_; }

  const std::vector<std::shared_ptr<ValueType>> &GetColumnTypes() const {
    return column_types_;
  }
};
} // namespace DB
