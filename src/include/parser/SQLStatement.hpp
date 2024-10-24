#pragma once

#include <memory>
#include <vector>

namespace DB {
enum class StatementType { CreateDatabase, DropDatabase };

class SQLStatement {
  StatementType type_;
  std::vector<std::shared_ptr<SQLStatement>> children_;

public:
  SQLStatement(StatementType type) : type_(type) {}
};
} // namespace DB