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

  // 用非所有权指针：在异步刷盘场景下 MemTable 仍保留在 immutable_table_ 中
  // 直到 SSTable 真正注册为止，以避免读路径出现 "miss window"
  static Status
  BuildSSTable(std::filesystem::path path, uint32_t &table_id,
               const std::vector<MemTable *> &memtables,
               const std::vector<std::shared_ptr<ValueType>> &column_types,
               uint16_t primary_key_idx, SSTableRef &sstable_meta);

  static Status StartCompaction(std::vector<SSTableRef> tables);

  static Status
  ReadSSTable(std::filesystem::path path, SSTableRef sstable_meta,
              const std::vector<std::shared_ptr<ValueType>> &column_types,
              std::shared_ptr<BufferPoolManager> buffer_pool);
};
} // namespace DB
