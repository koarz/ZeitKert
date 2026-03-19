#pragma once

#include "common/EnumClass.hpp"
#include "parser/AST.hpp"

#include <string>

namespace DB {
class DeleteQuery : public AST {
  std::string table_name_;

public:
  explicit DeleteQuery(std::string table_name)
      : AST(ASTNodeType::DeleteQuery), table_name_(std::move(table_name)) {}

  ~DeleteQuery() override = default;

  const std::string &GetTableName() const { return table_name_; }
};
} // namespace DB
