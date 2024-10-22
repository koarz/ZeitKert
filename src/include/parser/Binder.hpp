#pragma once

#include "parser/Parser.hpp"

namespace DB {
class Binder {
  Parser parser_;

public:
  Binder() = default;

  Status Parse(std::string_view query, std::shared_ptr<QueryContext> context,
               ResultSet &result_set);
};
} // namespace DB