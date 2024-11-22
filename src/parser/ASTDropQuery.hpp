#pragma once

#include "common/EnumClass.hpp"
#include "parser/AST.hpp"

#include <string>

namespace DB {
class DropQuery : public AST {
  DropType type_;
  std::string name_;

public:
  DropQuery(DropType type, std::string name)
      : AST(ASTNodeType::DropQuery), type_(type), name_(std::move(name)) {}
  ~DropQuery() override = default;

  DropType GetType() { return type_; }

  std::string &GetName() { return name_; }
};
} // namespace DB