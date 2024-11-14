#pragma once

#include "common/Context.hpp"
#include "parser/AST.hpp"
#include "parser/TokenIterator.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/statement/CreateStmt.hpp"
#include "parser/statement/SelectStmt.hpp"
#include "parser/statement/ShowStmt.hpp"
#include "parser/statement/UseStmt.hpp"
#include "storage/column/Column.hpp"

#include <memory>
namespace DB {
struct Transform {
  static std::shared_ptr<CreateStmt>
  TransCreateQuery(ASTPtr node, std::string &message,
                   std::shared_ptr<QueryContext> context);

  static std::shared_ptr<UseStmt>
  TransUseQuery(ASTPtr node, std::string &message,
                std::shared_ptr<QueryContext> context);

  static std::shared_ptr<ShowStmt>
  TransShowQuery(ASTPtr node, std::string &message,
                 std::shared_ptr<QueryContext> context);

  static std::shared_ptr<SelectStmt>
  TransSelectQuery(ASTPtr node, std::string &message,
                   std::shared_ptr<QueryContext> context);

private:
  static BoundExpressRef GetColumnExpress(TokenIterator &it, TokenIterator end,
                                          std::string &message);
};
} // namespace DB