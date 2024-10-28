#pragma once

#include "common/EnumClass.hpp"
#include "parser/AST.hpp"

namespace DB {
class UseQuery : public AST {
  std::string name_;

public:
  UseQuery() = default;
  UseQuery(const UseQuery &) = default;
  UseQuery(UseQuery &&) = default;
  ~UseQuery() override = default;

  explicit UseQuery(std::string name)
      : AST(ASTNodeType::UseQuery), name_(std::move(name)) {}

  std::string GetName() { return name_; }
};
} // namespace DB