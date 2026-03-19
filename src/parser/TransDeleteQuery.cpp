#include "parser/ASTDeleteQuery.hpp"
#include "parser/ASTToken.hpp"
#include "parser/Transform.hpp"
#include "parser/statement/DeleteStatement.hpp"

namespace DB {
std::shared_ptr<DeleteStatement>
Transform::TransDeleteQuery(ASTPtr node, std::string &message,
                            std::shared_ptr<QueryContext> context) {
  if (context->database_ == nullptr) {
    message = "you have not choice any database";
    return nullptr;
  }
  auto &delete_query = static_cast<DeleteQuery &>(*node);
  auto table_name = delete_query.GetTableName();
  auto table_meta = context->database_->GetTableMeta(table_name);
  if (table_meta == nullptr) {
    message = "the table not exist, please check table name";
    return nullptr;
  }
  auto res = std::make_shared<DeleteStatement>();
  res->table_ = table_meta;

  // 解析 WHERE 子句（如果存在）
  if (!delete_query.children_.empty() &&
      delete_query.children_[0]->GetNodeType() == ASTNodeType::Token) {
    auto &where_token = static_cast<ASTToken &>(*delete_query.children_[0]);
    auto where_it = where_token.Begin();
    std::vector<TableMetaRef> tables{table_meta};
    std::vector<BoundExpressRef> dummy_columns;
    auto where_expr =
        ParseExpression(where_it, where_token.End(), tables, dummy_columns, message);
    if (!message.empty()) {
      return nullptr;
    }
    res->where_condition_ = where_expr;
  }

  return res;
}
} // namespace DB
