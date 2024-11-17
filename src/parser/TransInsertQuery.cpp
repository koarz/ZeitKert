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
  auto tokens = static_cast<ASTToken &>(*insert_query.children_[0]);
  auto begin = tokens.Begin(), end = tokens.End();
  std::vector<BoundExpressRef> tuples;
  std::vector<TableMetaRef> tables{table_meta};
  while (begin != end) {
    auto t = begin;
    while ((++begin)->type != TokenType::ClosingRoundBracket && !begin->isEnd())
      ;
    auto tuple = GetTupleExpress(t, begin, tables, message);
    if (message.size() != 0) {
      return nullptr;
    }
    tuples.push_back(tuple);
    while ((++begin)->type != TokenType::OpeningRoundBracket && !begin->isEnd())
      ;
  }
  auto res = std::make_shared<InsertStatement>();
  res->table_ = table_meta;
  res->tuples_ = std::move(tuples);
  return res;
}
} // namespace DB