#pragma once

#include "common/Context.hpp"
#include "common/Instance.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/AST.hpp"
#include "parser/Checker.hpp"
#include "parser/Lexer.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/TokenIterator.hpp"

#include <memory>
#include <string_view>
#include <vector>

namespace DB {
class Parser : public Instance<Parser> {
public:
  Parser() = default;

  Status Parse(TokenIterator &iterator);

  Status ParseCreate(TokenIterator &iterator);

  Status ParseUse(TokenIterator &iterator);

  Status ParseShow(TokenIterator &iterator);

  Status ParseDrop(Lexer &lexer, std::shared_ptr<QueryContext> context,
                   ResultSet &result_set);

  ASTPtr tree_{nullptr};
};
} // namespace DB