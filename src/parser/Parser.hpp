#pragma once

#include "common/Instance.hpp"
#include "common/Status.hpp"
#include "parser/AST.hpp"
#include "parser/TokenIterator.hpp"

namespace DB {
class Parser : public Instance<Parser> {
public:
  Parser() = default;

  Status Parse(TokenIterator &iterator);

  Status ParseCreate(TokenIterator &iterator);

  Status ParseUse(TokenIterator &iterator);

  Status ParseShow(TokenIterator &iterator);

  Status ParseDrop(TokenIterator &iterator);

  Status ParseSelect(TokenIterator &iterator, bool stop_at_paren = false);

  Status ParseInsert(TokenIterator &iterator);

  Status ParseFlush(TokenIterator &iterator);

  ASTPtr tree_{nullptr};
};
} // namespace DB