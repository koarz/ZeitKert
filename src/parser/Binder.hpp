#pragma once

#include "common/Context.hpp"
#include "parser/Parser.hpp"
#include "parser/SQLStatement.hpp"

#include <memory>
#include <string_view>

namespace DB {
class Binder {
  Parser parser_;

  std::shared_ptr<SQLStatement> statement_;

public:
  Binder() = default;

  Status Parse(std::string_view query, std::shared_ptr<QueryContext> context,
               ResultSet &result_set);

  std::shared_ptr<SQLStatement> &GetStatement() { return statement_; }
};
} // namespace DB