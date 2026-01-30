#include "storage/lsmtree/ColumnReader.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"

#include <cstdint>
#include <cstring>

namespace DB {

void ColumnReader::ReadColumnFromRowGroup(
    const RowGroupMeta &rg, const Byte *base, size_t col_idx,
    const std::shared_ptr<ValueType> &type, ColumnPtr &column) {
  if (rg.row_count == 0 || col_idx >= rg.columns.size()) {
    return;
  }

  const auto &col = rg.columns[col_idx];
  const Byte *col_data = base + col.offset;

  switch (type->GetType()) {
  case ValueType::Type::Int: {
    auto *vec = static_cast<ColumnVector<int> *>(column.get());
    vec->Reserve(vec->Size() + rg.row_count);
    vec->InsertBulk(reinterpret_cast<const int *>(col_data), rg.row_count);
    break;
  }
  case ValueType::Type::Double: {
    auto *vec = static_cast<ColumnVector<double> *>(column.get());
    vec->Reserve(vec->Size() + rg.row_count);
    vec->InsertBulk(reinterpret_cast<const double *>(col_data), rg.row_count);
    break;
  }
  case ValueType::Type::String: {
    // String 列格式: [offset0][offset1]...[offsetN][offsetN+1][string_data...]
    // 共 row_count+1 个 offset，后接字符串数据
    const uint32_t *offsets = reinterpret_cast<const uint32_t *>(col_data);
    const char *str_data =
        reinterpret_cast<const char *>(offsets + rg.row_count + 1);
    size_t str_data_size = offsets[rg.row_count] - offsets[0];

    auto *str_col = static_cast<ColumnString *>(column.get());
    str_col->AppendBulk(offsets, rg.row_count + 1, str_data, str_data_size);
    break;
  }
  case ValueType::Type::Null: break;
  }
}

void ColumnReader::ReadColumnFromSSTable(const SSTableRef &sstable,
                                         size_t col_idx,
                                         const std::shared_ptr<ValueType> &type,
                                         ColumnPtr &column) {
  if (!sstable || !sstable->data_file_ || !sstable->data_file_->Valid()) {
    return;
  }

  const Byte *file_base = sstable->data_file_->Data();

  for (const auto &rg : sstable->rowgroups_) {
    const Byte *rg_base = file_base + static_cast<size_t>(rg.offset);
    ReadColumnFromRowGroup(rg, rg_base, col_idx, type, column);
  }
}

} // namespace DB
