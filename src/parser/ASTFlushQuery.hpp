#pragma once

#include "common/EnumClass.hpp"
#include "parser/AST.hpp"

#include <string>

namespace DB {
class FlushQuery : public AST {
  std::string table_name_;

public:
  explicit FlushQuery(std::string table_name)
      : AST(ASTNodeType::FlushQuery), table_name_(std::move(table_name)) {}

  ~FlushQuery() override = default;

  const std::string &GetTableName() const { return table_name_; }
};
} // namespace DB
