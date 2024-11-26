#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/SSTable.hpp"

#include <cstdint>
#include <filesystem>

namespace DB {
struct TableOperator {
  TableOperator() = delete;

  static Status BuildSSTable(std::filesystem::path path, uint32_t &table_id,
                             std::vector<MemTableRef> &memtables);

  static Status StartCompaction(std::vector<SSTableRef> tables);

  static Status ReadSSTable(std::filesystem::path path,
                            SSTableRef sstable_meta);
};
} // namespace DB