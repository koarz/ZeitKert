#include "parser/Transform.hpp"
#include "common/util/StringUtil.hpp"
#include "fmt/format.h"
#include "parser/Checker.hpp"
#include "parser/Lexer.hpp"
#include "parser/TokenIterator.hpp"
#include "parser/binder/BoundColumnMeta.hpp"
#include "parser/binder/BoundConstant.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "parser/binder/BoundTuple.hpp"
#include "type/Double.hpp"
#include "type/Int.hpp"
#include "type/String.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace DB {

// 将数值 token 封装为 BoundConstant（支持符号）
BoundExpressRef Transform::MakeNumericConstant(const Token &token,
                                               bool is_negative) {
  auto number = std::make_shared<BoundConstant>();
  std::string literal{token.begin, token.end};
  if (StringUtil::IsInteger(literal)) {
    number->type_ = std::make_shared<Int>();
    auto value = std::stoi(literal);
    number->value_.i32 = is_negative ? -value : value;
  } else {
    number->type_ = std::make_shared<Double>();
    auto value = std::stod(literal);
    number->value_.f64 = is_negative ? -value : value;
  }
  return number;
}

BoundExpressRef Transform::MakeStringConstant(const Token &token) {
  auto str = std::make_shared<BoundConstant>();
  str->type_ = std::make_shared<String>();
  str->value_.str = const_cast<char *>(token.begin);
  str->size_ = token.size();
  return str;
}

// 展开表的所有列（处理 '*'）
void Transform::AppendColumns(const TableMetaRef &table,
                              std::vector<BoundExpressRef> &columns) {
  for (auto &col_meta : table->GetColumns()) {
    columns.emplace_back(std::make_shared<BoundColumnMeta>(col_meta));
  }
}

// 处理 table.* 的列展开
bool Transform::AppendColumnsForTable(std::vector<TableMetaRef> &tables,
                                      const std::string &table_name,
                                      std::vector<BoundExpressRef> &columns,
                                      std::string &message) {
  auto it = std::find_if(tables.begin(), tables.end(),
                         [&table_name](const TableMetaRef &table) {
                           return table->GetTableName() == table_name;
                         });
  if (it == tables.end()) {
    message = fmt::format("table {} not exist", table_name);
    return false;
  }
  AppendColumns(*it, columns);
  return true;
}

ColumnMetaRef Transform::FindColumn(const TableMetaRef &table,
                                    const std::string &column_name) {
  for (const auto &col : table->GetColumns()) {
    if (col->name_ == column_name) {
      return col;
    }
  }
  return nullptr;
}

BoundExpressRef Transform::ResolveQualifiedColumn(
    std::vector<TableMetaRef> &tables, const std::string &table_name,
    const std::string &column_name, std::string &message) {
  for (auto &table : tables) {
    if (table->GetTableName() == table_name) {
      if (auto meta = FindColumn(table, column_name)) {
        return std::make_shared<BoundColumnMeta>(meta);
      }
      message = fmt::format("table {} doesn't contain column {}", table_name,
                            column_name);
      return nullptr;
    }
  }
  message = fmt::format("table {} not exist", table_name);
  return nullptr;
}

// 无前缀列名解析，检测多表下的歧义
BoundExpressRef
Transform::ResolveUnqualifiedColumn(std::vector<TableMetaRef> &tables,
                                    const std::string &column_name,
                                    std::string &message) {
  ColumnMetaRef matched{};
  for (auto &table : tables) {
    if (auto meta = FindColumn(table, column_name)) {
      if (matched != nullptr) {
        message = fmt::format(kAmbiguousColumnFmt, column_name);
        return nullptr;
      }
      matched = meta;
    }
  }
  if (matched == nullptr) {
    message = fmt::format("column {} not exist", column_name);
    return nullptr;
  }
  return std::make_shared<BoundColumnMeta>(matched);
}

// 跳过多余的逗号
bool Transform::SkipCommas(TokenIterator &it, const TokenIterator &end) {
  while (it < end && it->type == TokenType::Comma) {
    ++it;
  }
  return it < end;
}

BoundExpressRef Transform::ParseFunctionCall(std::string func_name,
                                             TokenIterator &it,
                                             TokenIterator end,
                                             std::vector<TableMetaRef> &tables,
                                             std::string &message) {
  // 解析函数参数
  if (!Checker::IsFunction(func_name)) {
    message = fmt::format("The Function {} Not Supported", func_name);
    return nullptr;
  }
  std::vector<BoundExpressRef> arguments;
  while (it < end && it->type != TokenType::ClosingRoundBracket) {
    auto arg = GetColumnExpress(++it, end, tables, arguments, message);
    if (!message.empty()) {
      return nullptr;
    }
    if (arg) {
      arguments.emplace_back(arg);
    }
  }
  return std::make_shared<BoundFunction>(Checker::GetFuncImpl(func_name),
                                         std::move(arguments));
}

BoundExpressRef Transform::ParseIdentifier(
    TokenIterator &it, TokenIterator end, std::vector<TableMetaRef> &tables,
    std::vector<BoundExpressRef> &columns, std::string &message) {
  // 处理列名 / table.col / 函数调用等情况
  std::string identifier{it->begin, it->end};
  auto next = it;
  ++next;

  if (next < end && next->type == TokenType::OpeningRoundBracket) {
    it = next;
    return ParseFunctionCall(identifier, it, end, tables, message);
  }

  if (next < end && next->type == TokenType::Dot) {
    auto column_token = next;
    ++column_token;
    if (!(column_token < end)) {
      message = fmt::format("unexpected end after {}.", identifier);
      return nullptr;
    }
    if (column_token->type == TokenType::Asterisk) {
      it = column_token;
      if (!AppendColumnsForTable(tables, identifier, columns, message)) {
        return nullptr;
      }
      return nullptr;
    }
    std::string column_name{column_token->begin, column_token->end};
    it = column_token;
    return ResolveQualifiedColumn(tables, identifier, column_name, message);
  }

  return ResolveUnqualifiedColumn(tables, identifier, message);
}

BoundExpressRef Transform::GetTupleExpress(TokenIterator begin,
                                           TokenIterator end,
                                           std::vector<TableMetaRef> &tables,
                                           std::string &message) {
  if (begin->type != TokenType::OpeningRoundBracket ||
      end->type != TokenType::ClosingRoundBracket) {
    return nullptr;
  }
  auto tuple = std::make_shared<BoundTuple>();
  while (++begin <= end) {
    auto col = GetColumnExpress(begin, end, tables, tuple->elements_, message);
    if (col) {
      tuple->elements_.emplace_back(col);
    }
  }
  return tuple;
}

BoundExpressRef Transform::GetColumnExpress(
    TokenIterator &it, TokenIterator end, std::vector<TableMetaRef> &tables,
    std::vector<BoundExpressRef> &columns, std::string &message) {
  if (!SkipCommas(it, end)) {
    return nullptr;
  }

  if (it->type == TokenType::Asterisk) {
    for (auto &table : tables) {
      AppendColumns(table, columns);
    }
    return nullptr;
  }

  if (it->type == TokenType::Minus) {
    auto next = it;
    ++next;
    if (next < end && next->type == TokenType::Number) {
      it = next;
      return MakeNumericConstant(*it, true);
    }
    message = "expect numeric literal after '-'";
    return nullptr;
  }

  if (it->type == TokenType::Number) {
    return MakeNumericConstant(*it, false);
  }

  if (it->type == TokenType::StringLiteral) {
    return MakeStringConstant(*it);
  }

  // 可能是函数、列名或者表名带列名（table.col）需要更多信息进一步判断
  if (it->type == TokenType::BareWord) {
    return ParseIdentifier(it, end, tables, columns, message);
  }
  return nullptr;
}
} // namespace DB
