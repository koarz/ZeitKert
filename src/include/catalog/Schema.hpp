#pragma once

#include "storage/column/ColumnWithNameType.hpp"
#include <cstddef>
#include <iostream>
#include <vector>
namespace DB {
class Schema;
using SchemaRef = std::shared_ptr<Schema>;

class Schema {

public:
  std::vector<ColumnWithNameTypeRef> &GetColumns() { return columns_; }

  void PrintColumns() {
    size_t max_row_num{};
    for (auto &c : columns_) {
      std::cout << '|' << c->GetColumnName();
      max_row_num = max_row_num > c->Size() ? max_row_num : c->Size();
    }
    std::cout << "|\n";
    for (int i = 0; i < max_row_num; i++) {
      std::cout << '|';
      for (auto &c : columns_) {
        std::cout << c->GetStrElement(i) << '|';
      }
      std::cout << '\n';
    }
  }

private:
  std::vector<ColumnWithNameTypeRef> columns_;
};
} // namespace DB