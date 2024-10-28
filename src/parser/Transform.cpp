#include "parser/Transform.hpp"
#include "common/EnumClass.hpp"
#include "parser/ASTCreateQuery.hpp"
#include "parser/ASTShowQuery.hpp"
#include "parser/ASTToken.hpp"
#include "parser/ASTUseQuery.hpp"
#include "parser/Checker.hpp"
#include "parser/Lexer.hpp"
#include "parser/statement/CreateStmt.hpp"
#include "parser/statement/ShowStmt.hpp"
#include "parser/statement/UseStmt.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/Int.hpp"

#include <memory>
#include <vector>

namespace DB {
std::shared_ptr<CreateStmt> Transform::TransCreateQuery(ASTPtr node) {
  auto &create_query = dynamic_cast<CreateQuery &>(*node.get());
  auto name = create_query.GetName();
  auto type = create_query.GetType();
  std::vector<std::shared_ptr<ColumnWithNameType>> columns;
  if (type == CreateType::TABLE) {
    auto &node_query =
        dynamic_cast<ASTToken &>(*create_query.children_[0].get());
    auto it = node_query.Begin();
    while (it->type != TokenType::ClosingRoundBracket) {
      if (it->type == TokenType::Comma) {
        ++it;
      }
      std::vector<Token> tokens;
      // token is ',' or ')'
      while (it->type != TokenType::Comma &&
             it->type != TokenType::ClosingRoundBracket) {
        if (it->type == TokenType::BareWord) {
          tokens.push_back(*it);
        }
        ++it;
      }
      // we will get all messages of one column
      // tokens[0] is col_name tokens[1] is val type
      // current version not upport others
      ColumnPtr column;
      std::shared_ptr<ValueType> type;
      auto col_name = std::string{tokens[0].begin, tokens[0].end};
      auto var_type = std::string{tokens[1].begin, tokens[1].end};
      if (Checker::IsType(var_type)) {
        if (var_type == "INT") {
          column = std::make_shared<ColumnVector<int>>();
          type = std::make_shared<Int>();
        } else {
          return nullptr;
        }
      }
      columns.emplace_back(
          std::make_shared<ColumnWithNameType>(column, col_name, type));
    }
  }
  return std::make_shared<CreateStmt>(type, name, columns);
}

std::shared_ptr<UseStmt> Transform::TransUseQuery(ASTPtr node) {
  auto &use_query = dynamic_cast<UseQuery &>(*node.get());

  auto name = use_query.GetName();

  return std::make_shared<UseStmt>(name);
}

std::shared_ptr<ShowStmt> Transform::TransShowQuery(ASTPtr node) {
  auto &show_query = dynamic_cast<ShowQuery &>(*node.get());

  return std::make_shared<ShowStmt>(show_query.GetShowType());
}
} // namespace DB