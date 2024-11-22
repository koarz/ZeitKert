#pragma once

#include "common/EnumClass.hpp"
#include "parser/AST.hpp"

namespace DB {
class ShowQuery : public AST {
  ShowType show_type_;

public:
  explicit ShowQuery(ShowType show_type)
      : AST(ASTNodeType::ShowQuery), show_type_(show_type) {}

  ~ShowQuery() override = default;

  ShowType GetShowType() { return show_type_; }
};
} // namespace DB