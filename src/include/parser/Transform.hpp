#pragma once

#include "common/Context.hpp"
#include "parser/AST.hpp"
#include "parser/TokenIterator.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/statement/CreateStatement.hpp"
#include "parser/statement/InsertStatement.hpp"
#include "parser/statement/SelectStatement.hpp"
#include "parser/statement/ShowStatement.hpp"
#include "parser/statement/UseStatement.hpp"

#include <memory>
namespace DB {
struct Transform {
  static std::shared_ptr<CreateStatement>
  TransCreateQuery(ASTPtr node, std::string &message,
                   std::shared_ptr<QueryContext> context);

  static std::shared_ptr<UseStatement>
  TransUseQuery(ASTPtr node, std::string &message,
                std::shared_ptr<QueryContext> context);

  static std::shared_ptr<ShowStatement>
  TransShowQuery(ASTPtr node, std::string &message,
                 std::shared_ptr<QueryContext> context);

  static std::shared_ptr<SelectStatement>
  TransSelectQuery(ASTPtr node, std::string &message,
                   std::shared_ptr<QueryContext> context);

  static std::shared_ptr<InsertStatement>
  TransInsertQuery(ASTPtr node, std::string &message,
                   std::shared_ptr<QueryContext> context);

private:
  static BoundExpressRef GetTupleExpress(TokenIterator begin, TokenIterator end,
                                         std::vector<TableMetaRef> &tables,
                                         std::string &message);

  static BoundExpressRef GetColumnExpress(TokenIterator &it, TokenIterator end,
                                          std::vector<TableMetaRef> &tables,
                                          std::vector<BoundExpressRef> &columns,
                                          std::string &message);
};
} // namespace DB