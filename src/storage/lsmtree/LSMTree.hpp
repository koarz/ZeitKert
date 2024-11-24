#pragma once

#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/IndexEngine.hpp"
#include "storage/column/Column.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace DB {
class LSMTree : public IndexEngine<Slice, Slice, SliceCompare> {
  std::shared_mutex latch_;
  std::shared_mutex immutable_latch_;

  bool write_log_;
  MemTableRef memtable_;
  // TODO:
  // the immutable table may reading by other threads
  // so we need safely delete them when immutable was full
  std::vector<MemTableRef> immutable_table_;
  // level and pages mapping
  std::unordered_map<uint32_t, std::vector<uint32_t>> levels_;
  // page id
  std::map<uint32_t, SSTableRef> sstables_;

  void ReadSSTableMeta();

public:
  LSMTree(std::filesystem::path column_path,
          std::shared_ptr<BufferPoolManager> buffer_pool_manager,
          std::shared_ptr<ValueType> value_type, bool write_log = true)
      : IndexEngine(SliceCompare{}, std::move(column_path),
                    std::move(buffer_pool_manager), std::move(value_type)),
        write_log_(write_log),
        memtable_(std::make_unique<MemTable>(column_path_, write_log)) {}

  ~LSMTree() override = default;

  Status Insert(const Slice &key, const Slice &value) override;

  Status Remove(const Slice &key) override;

  Status GetValue(const Slice &key, Slice *column) override;

  Status ScanColumn(ColumnPtr &res);

  size_t GetImmutableSize() { return immutable_table_.size(); }
};
} // namespace DB