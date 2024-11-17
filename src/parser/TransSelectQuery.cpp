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
    auto col = GetColumnExpress(it, node_query.End(), message);
    if (!message.empty()) {
      return nullptr;
    }
    if (col->expr_type_ == BoundExpressType::BoundColumnRef) {
      auto col_name = static_cast<BoundColumnRef &>(*col).GetColumnName();
      if (col_name == "*") {
        for (auto &table : res->from_) {
          for (auto &col_meta : table->GetColumns()) {
            columns.push_back(std::make_shared<BoundColumnMeta>(col_meta));
          }
        }
      } else {
        std::string table_name, column_name;
        switch (
            StringUtil::SpliteTableColumn(col_name, table_name, column_name)) {
        case -1: {
          message = fmt::format("your column: {} not correct", col_name);
          return nullptr;
        }
        case 0: {
          for (auto &table : res->from_) {
            columns.push_back(std::make_shared<BoundColumnMeta>(
                table->GetColumn(column_name)));
          }
          break;
        }
        case 1: {
          for (auto &table : res->from_) {
            if (table->GetTableName() == table_name) {
              columns.push_back(std::make_shared<BoundColumnMeta>(
                  table->GetColumn(column_name)));
              break;
            }
          }
          break;
        }
        }
      }
    } else {
      columns.push_back(col);
    }
    ++it;
  }

  res->columns_ = std::move(columns);
  return res;
}
} // namespace DB