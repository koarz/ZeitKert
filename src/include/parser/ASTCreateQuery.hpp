#pragma once

#include "common/EnumClass.hpp"
#include "parser/AST.hpp"

#include <string>

namespace DB {
class CreateQuery : public AST {
public:
  CreateQuery() = default;
  CreateQuery(const CreateQuery &) = default;
  CreateQuery(CreateQuery &&) = default;
  ~CreateQuery() override = default;

  CreateQuery(CreateType type, std::string name)
      : AST(ASTNodeType::CreateQuery), type_(type), name_(std::move(name)) {}

  CreateType GetType() { return type_; }

  std::string GetName() { return name_; }

private:
  CreateType type_;
  std::string name_;
};
} // namespace DB