#pragma once

#include "storage/MMapFile.hpp"
#include "storage/lsmtree/RowGroupMeta.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sys/types.h>
#include <vector>

namespace DB {
// clang-format off
// +---------------------------+-------------------------+-------------+
// |         数据区            |    RowGroup 元数据区    |    Footer   |
// +---------------------------+-------------------------+-------------+
// | rowgroup[0] ... rowgroup[n] | rowgroup_meta[...]    | fixed-size  |
// +---------------------------+-------------------------+-------------+
// 数据区  每个 RowGroup 使用 PAX 布局 并按 4KB 对齐
// 元数据区  记录 RowGroup 偏移 列分块 zonemap bloom 与最大主键
// Footer  记录 meta offset size rowgroup count column count pk index version magic
// clang-format on

struct SSTable {
  uint32_t sstable_id_;
  uint32_t rowgroup_count_{};
  uint16_t column_count_{};
  uint16_t primary_key_idx_{};
  std::vector<RowGroupMeta> rowgroups_;
  std::shared_ptr<MMapFile> data_file_;
};

using SSTableRef = std::shared_ptr<SSTable>;
} // namespace DB
