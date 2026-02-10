#pragma once

#include "parser/AST.hpp"
#include "parser/TokenIterator.hpp"

#include <optional>
#include <string>

namespace DB {
struct ASTTableFunction : public AST {
  std::string func_name_;
  std::optional<TokenIterator> args_begin_;
  std::optional<TokenIterator> args_end_;

  ASTTableFunction(std::string func_name, std::optional<TokenIterator> begin,
                   std::optional<TokenIterator> end)
      : AST(ASTNodeType::TableFunction), func_name_(std::move(func_name)),
        args_begin_(begin), args_end_(end) {}

  ~ASTTableFunction() override = default;
};
} // namespace DB
