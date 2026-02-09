#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/RowGroupMeta.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "type/ValueType.hpp"

#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>

namespace DB {
class RowGroupBuilder;

class SSTableBuilder {
  uint32_t table_id_{};
  std::filesystem::path path_;
  std::vector<std::shared_ptr<ValueType>> column_types_;
  uint16_t primary_key_idx_{};

  std::unique_ptr<std::ofstream> fs_;
  uint32_t data_size_{};
  std::vector<RowGroupMeta> rowgroups_;
  SSTableRef sstable_meta_;
  std::unique_ptr<RowGroupBuilder> rowgroup_builder_;

  Status FlushRowGroup();

public:
  SSTableBuilder(std::filesystem::path path, uint32_t table_num,
                 std::vector<std::shared_ptr<ValueType>> column_types,
                 uint16_t primary_key_idx);

  ~SSTableBuilder();

  [[nodiscard]]
  bool Add(const Slice &key, const Slice &row);

  Status Finish();

  SSTableRef BuildSSTableMeta() { return sstable_meta_; }
};
} // namespace DB
