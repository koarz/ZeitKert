#pragma once

#include "parser/AST.hpp"

namespace DB {
struct ASTSubquery : public AST {
  ASTPtr inner_select_;

  explicit ASTSubquery(ASTPtr inner_select)
      : AST(ASTNodeType::Subquery), inner_select_(std::move(inner_select)) {}

  ~ASTSubquery() override = default;
};
} // namespace DB
