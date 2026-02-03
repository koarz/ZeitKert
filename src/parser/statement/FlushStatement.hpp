#pragma once

#include "common/EnumClass.hpp"
#include "parser/SQLStatement.hpp"

#include <string>

namespace DB {
class FlushStatement : public SQLStatement {
  std::string table_name_;

public:
  explicit FlushStatement(std::string table_name)
      : SQLStatement(StatementType::FlushStatement),
        table_name_(std::move(table_name)) {}

  ~FlushStatement() override = default;

  const std::string &GetTableName() const { return table_name_; }
};
} // namespace DB
