#include "storage/lsmtree/ColumnReader.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"

#include <cstdint>
#include <cstring>
#include <string_view>

namespace DB {

void ColumnReader::ReadColumnFromRowGroup(
    const RowGroupMeta &rg, const Byte *base, size_t col_idx,
    const std::shared_ptr<ValueType> &type, ColumnPtr &column) {
  if (rg.row_count == 0 || col_idx >= rg.columns.size()) {
    return;
  }

  const auto &col = rg.columns[col_idx];
  const Byte *col_data = base + col.offset;

  // 如果列有 null，先读取 null bitmap
  size_t bitmap_size = 0;
  if (col.has_nulls) {
    bitmap_size = (rg.row_count + 7) / 8;
    column->SetNullBitmapRaw(reinterpret_cast<const uint8_t *>(col_data),
                             bitmap_size);
    col_data += bitmap_size;
  }

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

void ColumnReader::ReadColumnWithSelection(
    const RowGroupMeta &rg, const Byte *base, size_t col_idx,
    const std::shared_ptr<ValueType> &type, const RowGroupSelection &sel,
    ColumnPtr &column) {
  if (rg.row_count == 0 || col_idx >= rg.columns.size()) {
    return;
  }

  // 连续区间且覆盖整个 RowGroup，走快速路径
  if (sel.IsContiguous() && sel.start_row == 0 && sel.count == rg.row_count) {
    ReadColumnFromRowGroup(rg, base, col_idx, type, column);
    return;
  }

  const auto &col = rg.columns[col_idx];
  const Byte *col_data = base + col.offset;

  // 如果列有 null，读取 null bitmap 并跳过
  size_t bitmap_size = 0;
  const uint8_t *null_bitmap = nullptr;
  if (col.has_nulls) {
    bitmap_size = (rg.row_count + 7) / 8;
    null_bitmap = reinterpret_cast<const uint8_t *>(col_data);
    col_data += bitmap_size;
  }

  // 获取需要读取的行索引列表
  auto read_row = [&](uint32_t row_idx) {
    // 检查是否为 null
    if (null_bitmap) {
      if ((null_bitmap[row_idx / 8] >> (row_idx % 8)) & 1) {
        // null 行：插入占位值并标记 null
        switch (type->GetType()) {
        case ValueType::Type::Int:
          static_cast<ColumnVector<int> *>(column.get())->Insert(0);
          break;
        case ValueType::Type::Double:
          static_cast<ColumnVector<double> *>(column.get())->Insert(0.0);
          break;
        case ValueType::Type::String:
          static_cast<ColumnString *>(column.get())->Insert(std::string(""));
          break;
        case ValueType::Type::Null: break;
        }
        column->SetNull(column->Size() - 1);
        return;
      }
    }

    switch (type->GetType()) {
    case ValueType::Type::Int: {
      int v = 0;
      std::memcpy(&v, col_data + row_idx * sizeof(int), sizeof(int));
      static_cast<ColumnVector<int> *>(column.get())->Insert(v);
      break;
    }
    case ValueType::Type::Double: {
      double v = 0.0;
      std::memcpy(&v, col_data + row_idx * sizeof(double), sizeof(double));
      static_cast<ColumnVector<double> *>(column.get())->Insert(v);
      break;
    }
    case ValueType::Type::String: {
      const uint32_t *offsets = reinterpret_cast<const uint32_t *>(col_data);
      const char *str_data =
          reinterpret_cast<const char *>(offsets + rg.row_count + 1);
      uint32_t start = offsets[row_idx];
      uint32_t end = offsets[row_idx + 1];
      static_cast<ColumnString *>(column.get())
          ->Insert(std::string(str_data + start, end - start));
      break;
    }
    case ValueType::Type::Null: break;
    }
  };

  if (sel.IsContiguous()) {
    for (uint32_t i = 0; i < sel.count; i++) {
      read_row(sel.start_row + i);
    }
  } else {
    for (uint32_t idx : sel.rows) {
      read_row(idx);
    }
  }
}

void ColumnReader::EvalPredicateOnRowGroup(
    const RowGroupMeta &rg, const Byte *rg_base, const ScanPredicate &pred,
    std::vector<uint32_t> &matching_rows) {
  if (rg.row_count == 0 || pred.column_idx >= rg.columns.size()) {
    return;
  }

  const auto &col_meta = rg.columns[pred.column_idx];
  const Byte *col_data = rg_base + col_meta.offset;

  // 跳过 null bitmap
  size_t bitmap_size = 0;
  const uint8_t *null_bitmap = nullptr;
  if (col_meta.has_nulls) {
    bitmap_size = (rg.row_count + 7) / 8;
    null_bitmap = reinterpret_cast<const uint8_t *>(col_data);
    col_data += bitmap_size;
  }

  using Op = FunctionComparison::Operator;

  auto compare = [](Op op, auto lhs, auto rhs) -> bool {
    switch (op) {
    case Op::Less: return lhs < rhs;
    case Op::LessOrEquals: return lhs <= rhs;
    case Op::Greater: return lhs > rhs;
    case Op::GreaterOrEquals: return lhs >= rhs;
    case Op::Equals: return lhs == rhs;
    case Op::NotEquals: return lhs != rhs;
    }
    return false;
  };

  switch (pred.column_type) {
  case ValueType::Type::Int: {
    const int *data = reinterpret_cast<const int *>(col_data);
    int c = pred.const_int;
    for (uint32_t i = 0; i < rg.row_count; i++) {
      if (null_bitmap && ((null_bitmap[i / 8] >> (i % 8)) & 1)) {
        continue; // null 行不匹配
      }
      if (compare(pred.op, data[i], c)) {
        matching_rows.push_back(i);
      }
    }
    break;
  }
  case ValueType::Type::Double: {
    const double *data = reinterpret_cast<const double *>(col_data);
    double c = pred.const_double;
    for (uint32_t i = 0; i < rg.row_count; i++) {
      if (null_bitmap && ((null_bitmap[i / 8] >> (i % 8)) & 1)) {
        continue;
      }
      if (compare(pred.op, data[i], c)) {
        matching_rows.push_back(i);
      }
    }
    break;
  }
  case ValueType::Type::String: {
    const uint32_t *offsets = reinterpret_cast<const uint32_t *>(col_data);
    const char *str_data =
        reinterpret_cast<const char *>(offsets + rg.row_count + 1);
    const auto &c = pred.const_string;
    for (uint32_t i = 0; i < rg.row_count; i++) {
      if (null_bitmap && ((null_bitmap[i / 8] >> (i % 8)) & 1)) {
        continue;
      }
      uint32_t start = offsets[i];
      uint32_t end = offsets[i + 1];
      std::string_view sv(str_data + start, end - start);
      if (compare(pred.op, sv, std::string_view(c))) {
        matching_rows.push_back(i);
      }
    }
    break;
  }
  default: break;
  }
}

} // namespace DB
