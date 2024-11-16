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

  MemTableRef memtable_;
  std::vector<MemTableRef> immutable_table_;
  // level and pages mapping
  std::unordered_map<uint32_t, std::vector<page_id_t>> levels_;
  // page id
  std::map<page_id_t, SSTableRef> sstables_;

  void ReadSSTableMeta();

public:
  LSMTree(std::filesystem::path column_path,
          std::shared_ptr<BufferPoolManager> buffer_pool_manager,
          std::shared_ptr<ValueType> value_type)
      : IndexEngine(SliceCompare{}, std::move(column_path),
                    std::move(buffer_pool_manager), std::move(value_type)),
        memtable_(std::make_shared<MemTable>()) {}

  ~LSMTree() override = default;

  Status Insert(const Slice &key, const Slice &value) override;

  Status Remove(const Slice &key) override;

  Status GetValue(const Slice &key, Slice *column) override;

  Status ScanColumn(ColumnPtr &res);

  size_t GetImmutableSize() { return immutable_table_.size(); }
};
} // namespace DB