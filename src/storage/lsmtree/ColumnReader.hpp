#pragma once

#include "storage/column/Column.hpp"
#include "storage/lsmtree/RowGroupMeta.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/SelectionVector.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <memory>

namespace DB {

// 列读取器：直接从 RowGroup PAX 布局批量读取列数据
class ColumnReader {
public:
  // 从单个 RowGroup 读取指定列，追加到现有 Column
  static void ReadColumnFromRowGroup(const RowGroupMeta &rg, const Byte *base,
                                     size_t col_idx,
                                     const std::shared_ptr<ValueType> &type,
                                     ColumnPtr &column);

  // 从整个 SSTable 读取指定列，追加到现有 Column
  static void ReadColumnFromSSTable(const SSTableRef &sstable, size_t col_idx,
                                    const std::shared_ptr<ValueType> &type,
                                    ColumnPtr &column);

  // 从 RowGroup 读取指定行（根据 RowGroupSelection）
  static void ReadColumnWithSelection(const RowGroupMeta &rg, const Byte *base,
                                      size_t col_idx,
                                      const std::shared_ptr<ValueType> &type,
                                      const RowGroupSelection &sel,
                                      ColumnPtr &column);
};

} // namespace DB
