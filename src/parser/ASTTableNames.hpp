#pragma once

#include "parser/AST.hpp"

#include <vector>

namespace DB {
struct TableNames : public AST {
  std::vector<std::string> names_;

  TableNames(std::vector<std::string> names)
      : AST(ASTNodeType::TableNames), names_(std::move(names)) {}

  ~TableNames() override = default;
};
} // namespace DB