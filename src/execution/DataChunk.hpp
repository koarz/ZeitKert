#pragma once

#include "storage/column/ColumnWithNameType.hpp"

#include <cstddef>
#include <vector>

namespace DB {

// 向量化执行的标准批大小（参考 DuckDB STANDARD_VECTOR_SIZE）
constexpr size_t kVectorSize = 2048;

// 向量化执行的基本数据单元：一个批次内若干列共享同一行数
// 上游 Executor 每次 Next() 调用最多产出 kVectorSize 行的 DataChunk
class DataChunk {
public:
  DataChunk() = default;

  // 追加一个完整命名列，并以该列的 Size 设置批次行数（仅第一列）
  void AddColumn(ColumnWithNameTypeRef col) {
    if (columns_.empty()) {
      row_count_ = col->Size();
    }
    columns_.push_back(std::move(col));
  }

  // 追加一个命名列并显式指定批次行数（用于 Scan 等手动控制批大小的场景）
  void AddColumn(ColumnWithNameTypeRef col, size_t row_count) {
    if (columns_.empty()) {
      row_count_ = row_count;
    }
    columns_.push_back(std::move(col));
  }

  // 当前批次的行数
  size_t row_count() const { return row_count_; }

  // 显式覆盖批次行数（用于 Filter 等行数与列长度不一致的场景）
  void set_row_count(size_t n) { row_count_ = n; }

  // 当前批次的列数
  size_t column_count() const { return columns_.size(); }

  ColumnWithNameTypeRef &column(size_t i) { return columns_[i]; }
  const ColumnWithNameTypeRef &column(size_t i) const { return columns_[i]; }

  std::vector<ColumnWithNameTypeRef> &columns() { return columns_; }
  const std::vector<ColumnWithNameTypeRef> &columns() const { return columns_; }

  // 重置 chunk，准备接受下一个批次的数据
  void Reset() {
    columns_.clear();
    row_count_ = 0;
  }

  // 行数为 0 表示当前没有数据（一般用于 EOF 判定）
  bool IsEmpty() const { return row_count_ == 0; }

private:
  std::vector<ColumnWithNameTypeRef> columns_;
  size_t row_count_ = 0;
};

} // namespace DB
