#pragma once

#include "common/EnumClass.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace DB {

class SQLStatement {
public:
  static constexpr const StatementType TYPE = StatementType::INVALID_STATEMENT;

public:
  explicit SQLStatement(StatementType type) : type(type) {}
  virtual ~SQLStatement() {}

  StatementType type;

protected:
  SQLStatement(const SQLStatement &other) = default;

public:
};
} // namespace DB