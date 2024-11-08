#include "parser/Transform.hpp"
#include "common/EnumClass.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/ASTCreateQuery.hpp"
#include "parser/ASTSelectQuery.hpp"
#include "parser/ASTShowQuery.hpp"
#include "parser/ASTToken.hpp"
#include "parser/ASTUseQuery.hpp"
#include "parser/Checker.hpp"
#include "parser/Lexer.hpp"
#include "parser/TokenIterator.hpp"
#include "parser/binder/BoundConstant.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "parser/statement/CreateStmt.hpp"
#include "parser/statement/SelectStmt.hpp"
#include "parser/statement/ShowStmt.hpp"
#include "parser/statement/UseStmt.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/Double.hpp"
#include "type/Int.hpp"
#include "type/String.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace DB {
std::shared_ptr<CreateStmt> Transform::TransCreateQuery(ASTPtr node,
                                                        std::string &message) {
  auto &create_query = dynamic_cast<CreateQuery &>(*node);
  auto name = create_query.GetName();
  auto type = create_query.GetType();
  std::vector<std::shared_ptr<ColumnWithNameType>> columns;
  if (type == CreateType::TABLE) {
    auto &node_query = dynamic_cast<ASTToken &>(*create_query.children_[0]);
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
        } else if (var_type == "STRING") {
          column = std::make_shared<ColumnString>();
          type = std::make_shared<String>();
        } else if (var_type == "DOUBLE") {
          column = std::make_shared<ColumnVector<double>>();
          type = std::make_shared<Double>();
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

std::shared_ptr<UseStmt> Transform::TransUseQuery(ASTPtr node,
                                                  std::string &message) {
  auto &use_query = dynamic_cast<UseQuery &>(*node);

  auto name = use_query.GetName();

  return std::make_shared<UseStmt>(name);
}

std::shared_ptr<ShowStmt> Transform::TransShowQuery(ASTPtr node,
                                                    std::string &message) {
  auto &show_query = dynamic_cast<ShowQuery &>(*node);

  return std::make_shared<ShowStmt>(show_query.GetShowType());
}

std::shared_ptr<SelectStmt> Transform::TransSelectQuery(ASTPtr node,
                                                        std::string &message) {
  auto &select_query = dynamic_cast<SelectQuery &>(*node);

  auto &node_query = dynamic_cast<ASTToken &>(*select_query.children_[0]);
  std::vector<BoundExpressRef> columns;
  auto it = node_query.Begin();
  auto res = std::make_shared<SelectStmt>();

  // that's one query output column end of 'FROM'
  // we need parse constant or function or colname or table.col
  while (it != node_query.End()) {
    auto col = GetColumnExpress(it, node_query.End(), message);
    if (!message.empty()) {
      return nullptr;
    }
    columns.push_back(col);
    ++it;
  }

  res->columns_ = std::move(columns);
  return res;
}

BoundExpressRef Transform::GetColumnExpress(TokenIterator &it,
                                            TokenIterator end,
                                            std::string &message) {
  if (it == end) {
    return nullptr;
  }
  if (it->type == TokenType::Comma) {
    ++it;
    return GetColumnExpress(it, end, message);
  }

  if (it->type == TokenType::Number) {
    auto number = std::make_shared<BoundConstant>();
    std::string numstr{it->begin, it->end};
    if (StringUtil::IsInteger(numstr)) {
      number->type_ = std::make_shared<Int>();
      number->value_.i32 = std::stoi(numstr);
    } else {
      number->type_ = std::make_shared<Double>();
      number->value_.f64 = std::stod(numstr);
    }
    return number;
  }

  if (it->type == TokenType::StringLiteral) {
    auto str = std::make_shared<BoundConstant>();
    str->type_ = std::make_shared<String>();
    str->value_.str = const_cast<char *>(it->begin);
    str->size_ = it->size();
    return str;
  }

  // maybe function, col name or table.col so need get more info
  if (it->type == TokenType::BareWord) {
    // maybe function if not return error
    auto temp_begin = it;
    if ((++it)->type == TokenType::OpeningRoundBracket) {
      std::string func_name{temp_begin->begin, temp_begin->end};
      if (!Checker::IsFunction(func_name)) {
        message = std::format("The Function {} Not Supported", func_name);
        return nullptr;
      }
      std::vector<BoundExpressRef> arguments;
      // the arguments may column, function, constant
      // so we need to recursive to get columnexpr
      while ((++it)->type != TokenType::ClosingRoundBracket) {
        auto arg = GetColumnExpress(it, end, message);
        if (!message.empty()) {
          return nullptr;
        }
        arguments.push_back(arg);
      }
      return std::make_shared<BoundFunction>(Checker::GetFuncImpl(func_name),
                                             std::move(arguments));
    }
    // check is table.col ?
  }
  return nullptr;
}
} // namespace DB