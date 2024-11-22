#pragma once

#include "common/EnumClass.hpp"
#include "parser/AST.hpp"

namespace DB {
class SelectQuery : public AST {

public:
  explicit SelectQuery() : AST(ASTNodeType::SelectQuery) {}
};
} // namespace DB