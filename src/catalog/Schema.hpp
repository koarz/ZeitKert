#pragma once

#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"

#include <algorithm>
#include <cstddef>
#include <format>
#include <iostream>
#include <string>
#include <vector>
namespace DB {
class Schema;
using SchemaRef = std::shared_ptr<Schema>;

class Schema {

public:
  std::vector<ColumnWithNameTypeRef> &GetColumns() { return columns_; }

  void PrintColumns() {
    size_t max_row_num{}, column_num{};
    column_num = columns_.size();
    std::vector<size_t> max_elem_size;
    max_elem_size.reserve(column_num);

    // 先把所有数据转成字符串并存下来，同时计算 max_elem_size
    std::vector<std::vector<std::string>> str_data(column_num);
    for (size_t col_idx = 0; col_idx < column_num; col_idx++) {
      auto &c = columns_[col_idx];
      size_t row_count = c->Size();
      max_row_num = std::max(max_row_num, row_count);
      str_data[col_idx].reserve(row_count);

      size_t col_max = 4; // "Null"
      switch (c->GetValueType()->GetType()) {
      case ValueType::Type::Int: {
        auto &col = static_cast<ColumnVector<int> &>(*c->GetColumn());
        for (size_t k = 0; k < row_count; k++) {
          auto s = std::to_string(col[k]);
          if (s.size() > col_max) {
            col_max = s.size();
          }
          str_data[col_idx].push_back(std::move(s));
        }
        break;
      }
      case ValueType::Type::Double: {
        auto &col = static_cast<ColumnVector<double> &>(*c->GetColumn());
        for (size_t k = 0; k < row_count; k++) {
          auto s = std::to_string(col[k]);
          if (s.size() > col_max) {
            col_max = s.size();
          }
          str_data[col_idx].push_back(std::move(s));
        }
        break;
      }
      default: {
        for (size_t k = 0; k < row_count; k++) {
          auto s = c->GetStrElement(k);
          if (s.size() > col_max) {
            col_max = s.size();
          }
          str_data[col_idx].push_back(std::move(s));
        }
        break;
      }
      }
      max_elem_size.emplace_back(
          std::max({c->GetColumnName().size(), col_max, 4UL}) + 2);
    }

    std::string mid_line{"├"};
    // output top line
    std::cout << "╭";
    for (size_t i = 0; i < column_num; i++) {
      for (size_t j = 0; j < max_elem_size[i]; j++) {
        std::cout << "─";
        mid_line += "─";
      }
      if (i != column_num - 1) {
        std::cout << "┬";
        mid_line += "┼";
      }
    }
    std::cout << "╮\n";
    mid_line += "┤\n";
    // output column_name
    {
      size_t i{};
      std::cout << "│";
      for (auto &c : columns_) {
        auto &&s = c->GetColumnName();
        auto ss = (max_elem_size[i] - s.size()) / 2;
        std::cout << std::string(ss, ' ') << s
                  << std::string(max_elem_size[i] - s.size() - ss, ' ') << "│";
        i++;
      }
      std::cout << '\n';
    }
    // output data — 直接用已转好的字符串
    for (size_t i = 0; i < max_row_num; i++) {
      std::cout << mid_line;
      std::cout << "│";
      for (size_t j = 0; j < column_num; j++) {
        std::string s;
        if (i >= str_data[j].size()) {
          s = "Null";
        } else {
          s = str_data[j][i];
        }
        auto ss = (max_elem_size[j] - s.size()) / 2;
        std::cout << std::string(ss, ' ') << s
                  << std::string(max_elem_size[j] - s.size() - ss, ' ') << "│";
      }
      std::cout << '\n';
    }
    // output down line
    std::cout << "╰";
    for (size_t i = 0; i < column_num; i++) {
      for (size_t j = 0; j < max_elem_size[i]; j++) {
        std::cout << "─";
      }
      if (i != column_num - 1) {
        std::cout << "┴";
      }
    }
    std::cout << "╯\n";
    std::cout << std::format("Total Rows: {} Columns: {}\n", max_row_num,
                             column_num);
  }

private:
  std::vector<ColumnWithNameTypeRef> columns_;
};
} // namespace DB