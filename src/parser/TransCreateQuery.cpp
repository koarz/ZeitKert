#include "parser/ASTCreateQuery.hpp"
#include "parser/ASTToken.hpp"
#include "parser/Checker.hpp"
#include "parser/Transform.hpp"

namespace DB {
std::shared_ptr<CreateStatement>
Transform::TransCreateQuery(ASTPtr node, std::string &message,
                            std::shared_ptr<QueryContext> context) {
  auto &create_query = static_cast<CreateQuery &>(*node);
  auto name = create_query.GetName();
  auto type = create_query.GetType();
  std::vector<ColumnMetaRef> columns;
  std::string unique_key;
  if (type == CreateType::Table) {
    auto &node_query = static_cast<ASTToken &>(*create_query.children_[0]);
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
      std::shared_ptr<ValueType> type;
      auto col_name = std::string{tokens[0].begin, tokens[0].end};
      auto var_type = std::string{tokens[1].begin, tokens[1].end};
      if (Checker::IsType(var_type)) {
        if (var_type == "INT") {
          type = std::make_shared<Int>();
        } else if (var_type == "STRING") {
          type = std::make_shared<String>();
        } else if (var_type == "DOUBLE") {
          type = std::make_shared<Double>();
        } else {
          return nullptr;
        }
      }
      columns.emplace_back(std::make_shared<ColumnMeta>(col_name, type));
    }

    // 解析 UNIQUE KEY 子句
    if (create_query.children_.size() > 1) {
      auto &uk_node = static_cast<ASTToken &>(*create_query.children_[1]);
      auto uk_it = uk_node.Begin();
      if (uk_it->type == TokenType::BareWord) {
        unique_key = std::string{uk_it->begin, uk_it->end};
      }
    }

    if (unique_key.empty()) {
      message = "table has no primary key";
      return nullptr;
    }

    if (!unique_key.empty()) {
      ColumnMetaRef unique_col;
      for (const auto &col : columns) {
        if (col->name_ == unique_key) {
          unique_col = col;
          break;
        }
      }
      if (!unique_col) {
        message =
            "UNIQUE KEY column '" + unique_key + "' not found in table columns";
        return nullptr;
      }
      if (unique_col->type_->GetType() == ValueType::Type::Double) {
        message = "UNIQUE KEY column '" + unique_key + "' cannot be DOUBLE";
        return nullptr;
      }
    }
  }
  return std::make_shared<CreateStatement>(type, name, columns, unique_key);
}

} // namespace DB
