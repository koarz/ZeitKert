#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace DB {
enum class StatementType {
  INVALID_STATEMENT,
  CREATE_STATEMENT,
  SELECT_STATEMENT,
};

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