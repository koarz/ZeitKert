#pragma once

#include <cstdint>
#include <vector>

namespace DB {

// 数据来源类型
enum class DataSource : uint8_t {
  MemTable,
  Immutable,
  SSTable,
};

// 表示一个 RowGroup 内被选中的行
struct RowGroupSelection {
  DataSource source;
  uint32_t source_id;    // Immutable 索引 或 SSTable ID
  uint32_t rowgroup_idx; // SSTable 的 RowGroup 索引，MemTable/Immutable 为 0

  // 选中的行，两种表示方式：
  // 1. 连续区间：start_row + count，rows 为空
  // 2. 离散行：rows 非空，存储具体行索引
  uint32_t start_row{0};
  uint32_t count{0};
  std::vector<uint32_t> rows; // 离散行索引，为空时使用 start_row + count

  // 判断是否为连续区间
  bool IsContiguous() const { return rows.empty(); }

  // 获取选中的行数
  uint32_t RowCount() const {
    return IsContiguous() ? count : static_cast<uint32_t>(rows.size());
  }
};

// Selection Vector：记录去重后需要读取的数据位置
class SelectionVector {
  std::vector<RowGroupSelection> selections_;
  size_t total_rows_{0};

public:
  SelectionVector() = default;

  // 添加一个连续区间
  void AddContiguous(DataSource source, uint32_t source_id,
                     uint32_t rowgroup_idx, uint32_t start_row,
                     uint32_t count) {
    if (count == 0)
      return;
    selections_.push_back(
        {source, source_id, rowgroup_idx, start_row, count, {}});
    total_rows_ += count;
  }

  // 添加离散的行
  void AddRows(DataSource source, uint32_t source_id, uint32_t rowgroup_idx,
               std::vector<uint32_t> rows) {
    if (rows.empty())
      return;
    total_rows_ += rows.size();
    selections_.push_back(
        {source, source_id, rowgroup_idx, 0, 0, std::move(rows)});
  }

  // 添加单行
  void AddRow(DataSource source, uint32_t source_id, uint32_t rowgroup_idx,
              uint32_t row_idx) {
    // 尝试合并到最后一个 selection
    if (!selections_.empty()) {
      auto &last = selections_.back();
      if (last.source == source && last.source_id == source_id &&
          last.rowgroup_idx == rowgroup_idx) {
        if (last.IsContiguous()) {
          // 连续区间，检查能否扩展
          if (row_idx == last.start_row + last.count) {
            last.count++;
            total_rows_++;
            return;
          }
          // 不能扩展，转为离散模式
          last.rows.reserve(last.count + 1);
          for (uint32_t i = 0; i < last.count; i++) {
            last.rows.push_back(last.start_row + i);
          }
          last.rows.push_back(row_idx);
          last.start_row = 0;
          last.count = 0;
          total_rows_++;
          return;
        } else {
          // 已经是离散模式，直接添加
          last.rows.push_back(row_idx);
          total_rows_++;
          return;
        }
      }
    }
    // 新建一个连续区间
    selections_.push_back({source, source_id, rowgroup_idx, row_idx, 1, {}});
    total_rows_++;
  }

  const std::vector<RowGroupSelection> &GetSelections() const {
    return selections_;
  }

  size_t TotalRows() const { return total_rows_; }

  bool Empty() const { return total_rows_ == 0; }

  void Clear() {
    selections_.clear();
    total_rows_ = 0;
  }
};

} // namespace DB
