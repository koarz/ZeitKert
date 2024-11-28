#pragma once

#include "common/Status.hpp"
#include "storage/IndexEngine.hpp"
#include "storage/column/Column.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/TableOperator.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
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
  // TODO:
  // the immutable table may reading by other threads
  // so we need safely delete them when immutable was full
  std::vector<MemTableRef> immutable_table_;
  std::map<uint32_t, SSTableRef> sstables_;

public:
  LSMTree(std::filesystem::path column_path, uint32_t table_number,
          std::shared_ptr<BufferPoolManager> buffer_pool_manager,
          std::shared_ptr<ValueType> value_type, bool write_log = true)
      : IndexEngine(SliceCompare{}, std::move(column_path),
                    std::move(buffer_pool_manager), std::move(value_type)),
        table_number_(table_number), write_log_(write_log),
        memtable_(std::make_unique<MemTable>(column_path_, write_log)) {
    for (int i = 0; i < table_number; i++) {
      auto &table_meta = sstables_[i] = std::make_shared<SSTable>();
      table_meta->sstable_id_ = i;
      std::ignore = TableOperator::ReadSSTable(column_path_, table_meta);
    }
  }

  ~LSMTree() override = default;

  Status Insert(const Slice &key, const Slice &value) override;

  Status Remove(const Slice &key) override;

  Status GetValue(const Slice &key, Slice *column) override;

  Status ScanColumn(ColumnPtr &res);

  size_t GetImmutableSize() { return immutable_table_.size(); }

  uint32_t GetTableNum() { return table_number_; }
};
} // namespace DB