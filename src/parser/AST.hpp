#pragma once

#include "common/EnumClass.hpp"
#include <memory>
#include <vector>

namespace DB {
class AST;

using ASTPtr = std::shared_ptr<AST>;
using ASTs = std::vector<ASTPtr>;

class AST : public std::enable_shared_from_this<AST> {
  ASTNodeType node_type_ = ASTNodeType::InValid;

public:
  ASTs children_;

  virtual ~AST() = default;
  AST() = default;
  AST(const AST &) = default;
  AST &operator=(const AST &) = default;

  AST(ASTNodeType node_type) : node_type_(node_type) {}
  ASTNodeType GetNodeType() { return node_type_; }

  ASTPtr ptr() { return shared_from_this(); }
};
} // namespace DB