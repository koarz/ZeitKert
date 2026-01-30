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
// ============================================================================
//                         SSTable 文件存储格式
// ============================================================================
//
// 整体布局:
// ┌─────────────────────────────────────┐  offset: 0
// │           数据区 (Data)              │
// │  RowGroup[0] ... RowGroup[N]        │
// ├─────────────────────────────────────┤  offset: meta_offset
// │        元数据区 (Metadata)           │
// │  RowGroupMeta[0] ... RowGroupMeta[N]│
// ├─────────────────────────────────────┤  offset: file_size - 28
// │          Footer (28 bytes)          │
// └─────────────────────────────────────┘
//
// ============================================================================
//                         1. 数据区 - RowGroup (PAX 布局)
// ============================================================================
//
// 每个 RowGroup 按 4KB 对齐:
// ┌──────────────────────────────────────────────┐
// │ Column[0] | Column[1] | ... | Column[N]      │
// ├──────────────────────────────────────────────┤
// │ Padding (填充到 4KB 对齐)                     │
// └──────────────────────────────────────────────┘
//
// Column 内部格式 (R = row_count):
//
//   Int 列:
//   ┌────────┬────────┬─────┬────────┐
//   │ val[0] │ val[1] │ ... │ val[R] │  每个 4 bytes
//   └────────┴────────┴─────┴────────┘
//
//   Double 列:
//   ┌────────┬────────┬─────┬────────┐
//   │ val[0] │ val[1] │ ... │ val[R] │  每个 8 bytes
//   └────────┴────────┴─────┴────────┘
//
//   String 列:
//   ┌───────────┬───────────┬─────┬─────────────┬──────────────────┐
//   │ offset[0] │ offset[1] │ ... │ offset[R+1] │ string data ...  │
//   │ u32       │ u32       │     │ u32         │ 连续拼接          │
//   └───────────┴───────────┴─────┴─────────────┴──────────────────┘
//   (通过 offset[i+1] - offset[i] 计算第 i 行字符串长度)
//
// ============================================================================
//                         2. 元数据区 - RowGroupMeta
// ============================================================================
//
// 每个 RowGroupMeta 连续序列化:
// ┌────────────────────────────────────────────────────────┐
// │ offset          (u32)    RowGroup 在数据区的偏移        │
// │ row_count       (u32)    行数                          │
// ├────────────────────────── 重复 column_count 次 ─────────┤
// │   col.offset    (u32)    列在 RowGroup 内的偏移         │
// │   col.size      (u32)    列数据字节数                   │
// │   has_value     (u8)     ZoneMap 是否有值               │
// │   ZoneMap:                                              │
// │     Int:    min (4B) + max (4B)                         │
// │     Double: min (8B) + max (8B)                         │
// │     String: min_len (u16) + min_data + max_len (u16) + max_data │
// ├─────────────────────────────────────────────────────────┤
// │ bloom_size      (u32)    布隆过滤器字节数               │
// │ bloom_data      (bloom_size bytes)                     │
// │ key_size        (u32)    最大主键字节数                  │
// │ max_key         (key_size bytes)                        │
// └────────────────────────────────────────────────────────┘
//
// ============================================================================
//                         3. Footer (固定 28 bytes)
// ============================================================================
//
// ┌──────────────────────────────────────┐
// │ meta_offset      (u32)  元数据区偏移  │
// │ meta_size        (u32)  元数据区大小  │
// │ rowgroup_count   (u32)  RowGroup 数量 │
// │ column_count     (u16)  列数          │
// │ primary_key_idx  (u16)  主键列索引    │
// │ version          (u16)  版本号 = 1    │
// │ reserved         (u16)  保留 = 0      │
// │ magic            (u32)  0x5A4B5254    │
// └──────────────────────────────────────┘
//
// ============================================================================
//                         读取路径
// ============================================================================
//
// 1. 读 Footer (文件末尾 28 bytes) -> 得到 meta_offset, meta_size
// 2. 读 Metadata (meta_offset 处) -> 反序列化所有 RowGroupMeta
// 3. mmap 整个文件 -> 通过 RowGroupMeta.offset 直接访问数据
//
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
