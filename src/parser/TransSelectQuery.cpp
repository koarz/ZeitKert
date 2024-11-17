#include "common/EnumClass.hpp"
#include "common/util/StringUtil.hpp"
#include "fmt/format.h"
#include "parser/AST.hpp"
#include "parser/ASTSelectQuery.hpp"
#include "parser/ASTTableNames.hpp"
#include "parser/ASTToken.hpp"
#include "parser/Transform.hpp"
#include "parser/binder/BoundColumnMeta.hpp"
#include "parser/binder/BoundColumnRef.hpp"
#include "parser/statement/SelectStatement.hpp"
#include <memory>

namespace DB {
std::shared_ptr<SelectStatement>
Transform::TransSelectQuery(ASTPtr node, std::string &message,
                            std::shared_ptr<QueryContext> context) {
  auto &select_query = static_cast<SelectQuery &>(*node);
  auto res = std::make_shared<SelectStatement>();
  // handle from table first
  if (select_query.children_.size() > 1) {
    auto &tables = static_cast<TableNames &>(*select_query.children_[1]);
    for (auto &s : tables.names_) {
      auto table_meta = context->database_->GetTableMeta(s);
      if (table_meta == nullptr) {
        message = "the table not exist, please check table name";
        return nullptr;
      }
      res->from_.push_back(table_meta);
    }
  }

  auto &node_query = static_cast<ASTToken &>(*select_query.children_[0]);
  std::vector<BoundExpressRef> columns;
  auto it = node_query.Begin();

  // that's one query output column end of 'FROM'
  // we need parse constant or function or colname or table.col
  while (it < node_query.End()) {
    auto col =
        GetColumnExpress(it, node_query.End(), res->from_, columns, message);
    if (!message.empty()) {
      return nullptr;
    }
    if (col) {
      columns.push_back(col);
    }
    ++it;
  }

  res->columns_ = std::move(columns);
  return res;
}
} // namespace DB