#pragma once

#include "storage/column/ColumnWithNameType.hpp"

#include <cstddef>
#include <format>
#include <iostream>
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
    for (auto &c : columns_) {
      max_row_num = max_row_num > c->Size() ? max_row_num : c->Size();
      max_elem_size.emplace_back(c->GetMaxElementSize() + 2);
    }
    std::string mid_line{"├"};
    // output top line
    std::cout << "╭";
    for (int i = 0; i < column_num; i++) {
      for (int j = 0; j < max_elem_size[i]; j++) {
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
      int i{};
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
    // output data
    for (int i = 0; i < max_row_num; i++) {
      std::cout << mid_line;
      std::cout << "│";
      int j{};
      for (auto &c : columns_) {
        auto &&s = c->GetStrElement(i);
        auto ss = (max_elem_size[j] - s.size()) / 2;
        std::cout << std::string(ss, ' ') << s
                  << std::string(max_elem_size[j] - s.size() - ss, ' ') << "│";
        j++;
      }
      std::cout << '\n';
    }
    // output down line
    std::cout << "╰";
    for (int i = 0; i < column_num; i++) {
      for (int j = 0; j < max_elem_size[i]; j++) {
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