#pragma once

#include "parser/Parser.hpp"
#include "parser/SQLStatement.hpp"
#include <memory>
#include <string_view>
#include <vector>

namespace DB {
class Binder {
  Parser parser_;

  std::vector<std::shared_ptr<SQLStatement>> statements_;

public:
  Binder() = default;

  Status Parse(std::string_view query, std::shared_ptr<QueryContext> context,
               ResultSet &result_set);

  std::vector<std::shared_ptr<SQLStatement>> &GetStatements() {
    return statements_;
  }
};
} // namespace DB