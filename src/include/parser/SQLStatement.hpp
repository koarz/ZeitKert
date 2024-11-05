#pragma once

#include "common/EnumClass.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace DB {

class SQLStatement {
public:
  explicit SQLStatement(StatementType type) : type(type) {}
  virtual ~SQLStatement() {}

  StatementType type;

protected:
  SQLStatement(const SQLStatement &other) = default;
};
} // namespace DB