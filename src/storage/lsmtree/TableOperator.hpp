#pragma once

#include "buffer/BufferPoolManager.hpp"
#include "common/Status.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "type/ValueType.hpp"

#include <cstdint>
#include <filesystem>

namespace DB {
struct TableOperator {
  TableOperator() = delete;

  static Status
  BuildSSTable(std::filesystem::path path, uint32_t &table_id,
               std::vector<MemTableRef> &memtables,
               const std::vector<std::shared_ptr<ValueType>> &column_types,
               uint16_t primary_key_idx, SSTableRef &sstable_meta);

  static Status StartCompaction(std::vector<SSTableRef> tables);

  static Status
  ReadSSTable(std::filesystem::path path, SSTableRef sstable_meta,
              const std::vector<std::shared_ptr<ValueType>> &column_types,
              std::shared_ptr<BufferPoolManager> buffer_pool);
};
} // namespace DB
