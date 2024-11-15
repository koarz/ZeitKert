#pragma once

#include "parser/AST.hpp"

namespace DB {
// if InsertQuery is insert into values, it just have a token child
// the insert table lookup will be supported in the future
class InsertQuery : public AST {
  std::string insert_into_;

public:
  explicit InsertQuery(std::string insert_into)
      : AST(ASTNodeType::InsertQuery), insert_into_(std::move(insert_into)) {}

  std::string GetInsertIntoName() { return insert_into_; }
};
} // namespace DB