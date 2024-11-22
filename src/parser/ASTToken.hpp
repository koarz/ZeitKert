#pragma once

#include "parser/AST.hpp"
#include "parser/TokenIterator.hpp"

#include <optional>

namespace DB {
class ASTToken : public AST {
public:
  ASTToken() = default;
  ASTToken(const ASTToken &) = default;
  ASTToken(ASTToken &&) = default;
  ~ASTToken() override = default;

  ASTToken(std::optional<TokenIterator> begin = std::nullopt,
           std::optional<TokenIterator> end = std::nullopt)
      : AST(ASTNodeType::Token), begin_(begin), end_(end) {}

  TokenIterator Begin() { return begin_.value(); }

  TokenIterator End() { return end_.value(); }

private:
  std::optional<TokenIterator> begin_;
  std::optional<TokenIterator> end_;
};
} // namespace DB