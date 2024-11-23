#pragma once

#include "parser/AST.hpp"

namespace DB {
// if InsertQuery is insert into values, it just have a token child
// the insert table lookup will be supported in the future
class InsertQuery : public AST {
  std::string insert_into_;
  ASTPtr select_;

public:
  explicit InsertQuery(std::string insert_into, ASTPtr select)
      : AST(ASTNodeType::InsertQuery), insert_into_(std::move(insert_into)),
        select_(std::move(select)) {}

  std::string GetInsertIntoName() { return insert_into_; }

  ASTPtr GetSelect() { return select_; }
};
} // namespace DB