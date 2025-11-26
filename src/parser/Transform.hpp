#pragma once

#include "common/Context.hpp"
#include "parser/AST.hpp"
#include "parser/TokenIterator.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/statement/CreateStatement.hpp"
#include "parser/statement/DropStatement.hpp"
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

  static std::shared_ptr<DropStatement>
  TransDropQuery(ASTPtr node, std::string &message,
                 std::shared_ptr<QueryContext> context);

private:
  static constexpr const char *kAmbiguousColumnFmt =
      "column {} is ambiguous, please use table.column";

  static BoundExpressRef GetTupleExpress(TokenIterator begin, TokenIterator end,
                                         std::vector<TableMetaRef> &tables,
                                         std::string &message);

  static BoundExpressRef GetColumnExpress(TokenIterator &it, TokenIterator end,
                                          std::vector<TableMetaRef> &tables,
                                          std::vector<BoundExpressRef> &columns,
                                          std::string &message);

  static bool SkipCommas(TokenIterator &it, const TokenIterator &end);

  static BoundExpressRef MakeNumericConstant(const Token &token,
                                             bool is_negative);
  static BoundExpressRef MakeStringConstant(const Token &token);

  static void AppendColumns(const TableMetaRef &table,
                            std::vector<BoundExpressRef> &columns);

  static bool AppendColumnsForTable(std::vector<TableMetaRef> &tables,
                                    const std::string &table_name,
                                    std::vector<BoundExpressRef> &columns,
                                    std::string &message);

  static ColumnMetaRef FindColumn(const TableMetaRef &table,
                                  const std::string &column_name);

  static BoundExpressRef
  ResolveQualifiedColumn(std::vector<TableMetaRef> &tables,
                         const std::string &table_name,
                         const std::string &column_name, std::string &message);

  static BoundExpressRef
  ResolveUnqualifiedColumn(std::vector<TableMetaRef> &tables,
                           const std::string &column_name,
                           std::string &message);

  static BoundExpressRef ParseFunctionCall(std::string func_name,
                                           TokenIterator &it, TokenIterator end,
                                           std::vector<TableMetaRef> &tables,
                                           std::string &message);

  static BoundExpressRef ParseIdentifier(TokenIterator &it, TokenIterator end,
                                         std::vector<TableMetaRef> &tables,
                                         std::vector<BoundExpressRef> &columns,
                                         std::string &message);
};
} // namespace DB
