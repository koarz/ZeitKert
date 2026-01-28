#include "parser/ASTInsertQuery.hpp"
#include "parser/ASTToken.hpp"
#include "parser/Lexer.hpp"
#include "parser/Transform.hpp"
#include "parser/statement/InsertStatement.hpp"

namespace DB {
std::shared_ptr<InsertStatement>
Transform::TransInsertQuery(ASTPtr node, std::string &message,
                            std::shared_ptr<QueryContext> context) {
  if (context->database_ == nullptr) {
    message = "you have not choice any database";
    return nullptr;
  }
  auto &insert_query = static_cast<InsertQuery &>(*node);
  auto name = insert_query.GetInsertIntoName();
  auto table_meta = context->database_->GetTableMeta(name);
  if (table_meta == nullptr) {
    message = "the table not exist, please check table name";
    return nullptr;
  }
  auto res = std::make_shared<InsertStatement>();
  if (insert_query.IsBulk()) {
    if (insert_query.GetBulkRows() == 0) {
      message = "bulk insert rows must be greater than 0";
      return nullptr;
    }
    res->table_ = table_meta;
    res->SetBulkRows(insert_query.GetBulkRows());
    return res;
  }
  if (auto select_query = insert_query.GetSelect(); select_query != nullptr) {
    // select
    res->select_ = TransSelectQuery(select_query, message, context);
    if (res->select_ == nullptr) {
      return nullptr;
    }
    res->table_ = table_meta;
  } else {
    // values
    auto tokens = static_cast<ASTToken &>(*insert_query.children_[0]);
    auto begin = tokens.Begin(), end = tokens.End();
    std::vector<BoundExpressRef> tuples;
    std::vector<TableMetaRef> tables{table_meta};
    while (begin != end) {
      auto t = begin;
      while ((++begin)->type != TokenType::ClosingRoundBracket &&
             !begin->isEnd())
        ;
      auto tuple = GetTupleExpress(t, begin, tables, message);
      if (message.size() != 0) {
        return nullptr;
      }
      tuples.push_back(tuple);
      while ((++begin)->type != TokenType::OpeningRoundBracket &&
             !begin->isEnd())
        ;
    }
    res->table_ = table_meta;
    res->tuples_ = std::move(tuples);
  }
  return res;
}
} // namespace DB
