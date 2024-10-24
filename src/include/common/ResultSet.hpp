#pragma once

#include <iomanip>
#include <iostream>
#include <ostream>
#include <string_view>
#include <vector>
namespace DB {
class ResultSet {
public:
  struct Row {
    std::string message;
  };

  static int calculateMaxWidth(const std::vector<Row> &rows,
                               std::string_view &columnName) {
    int maxWidth = columnName.length();
    for (const auto &row : rows) {
      if (row.message.length() > maxWidth) {
        maxWidth = row.message.length();
      }
    }
    return maxWidth;
  }
  static void PrintResult(const std::vector<Row> &row,
                          std::string_view sql_stmt) {
    int maxWidth = calculateMaxWidth(row, sql_stmt);

    std::cout << "+" << std::string(maxWidth + 2, '-') << "+" << std::endl;
    std::cout << "| " << std::left << std::setw(maxWidth) << sql_stmt << " |"
              << std::endl;
    std::cout << "+" << std::string(maxWidth + 2, '-') << "+" << std::endl;

    for (const auto &row : row) {
      std::cout << "| " << std::left << std::setw(maxWidth) << row.message
                << " |" << std::endl;
    }

    std::cout << "+" << std::string(maxWidth + 2, '-') << "+" << std::endl;
  }
};
} // namespace DB