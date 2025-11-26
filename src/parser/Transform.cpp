#include "parser/Transform.hpp"
#include "common/util/StringUtil.hpp"
#include "fmt/format.h"
#include "function/FunctionArithmetic.hpp"
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
  // 去掉字符串字面量的前后引号
  if (token.size() >= 2 && *token.begin == '\'' && *(token.end - 1) == '\'') {
    str->value_.str = const_cast<char *>(token.begin + 1);
    str->size_ = token.size() - 2;
  } else {
    str->value_.str = const_cast<char *>(token.begin);
    str->size_ = token.size();
  }
  return str;
}

BoundExpressRef Transform::MakeZeroConstant() {
  auto zero = std::make_shared<BoundConstant>();
  zero->type_ = std::make_shared<Int>();
  zero->value_.i32 = 0;
  return zero;
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
  // it 当前指向 '('，移动到第一个参数
  ++it;
  // 处理空参数列表：func()
  if (it < end && it->type == TokenType::ClosingRoundBracket) {
    return std::make_shared<BoundFunction>(Checker::GetFuncImpl(func_name),
                                           std::move(arguments));
  }
  // 解析参数列表
  std::vector<BoundExpressRef> dummy_columns;
  while (it < end) {
    // 直接调用 ParseExpression 解析参数，避免 GetColumnExpress 跳过逗号
    auto arg = ParseExpression(it, end, tables, dummy_columns, message);
    if (!message.empty()) {
      return nullptr;
    }
    if (arg) {
      arguments.emplace_back(arg);
    }
    // 检查下一个 token
    auto next = it;
    ++next;
    if (!(next < end)) {
      message = "unexpected end, expect ) or ,";
      return nullptr;
    }
    if (next->type == TokenType::ClosingRoundBracket) {
      it = next; // 移动到 ')'
      break;
    } else if (next->type == TokenType::Comma) {
      it = next; // 移动到 ','
      ++it;      // 跳过逗号，准备解析下一个参数
    } else {
      message = fmt::format("unexpected token {}, expect ) or ,",
                            getTokenName(next->type));
      return nullptr;
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

int Transform::GetBinaryPrecedence(TokenType type) {
  switch (type) {
  case TokenType::Plus:
  case TokenType::Minus: return 1;
  case TokenType::Asterisk:
  case TokenType::Slash: return 2;
  default: return -1;
  }
}

std::shared_ptr<ValueType>
Transform::GetExpressType(const BoundExpressRef &express) {
  if (!express) {
    return nullptr;
  }
  switch (express->expr_type_) {
  case BoundExpressType::BoundConstant:
    return static_cast<BoundConstant &>(*express).type_;
  case BoundExpressType::BoundFunction:
    return static_cast<BoundFunction &>(*express)
        .GetFunction()
        ->GetResultType();
  case BoundExpressType::BoundColumnMeta:
    return static_cast<BoundColumnMeta &>(*express).GetColumnMeta()->type_;
  default: break;
  }
  return nullptr;
}

std::shared_ptr<ValueType>
Transform::DeduceNumericType(const BoundExpressRef &lhs,
                             const BoundExpressRef &rhs, std::string &message) {
  auto left_type = GetExpressType(lhs);
  auto right_type = GetExpressType(rhs);
  if (left_type == nullptr || right_type == nullptr) {
    message = "failed to deduce arithmetic type";
    return nullptr;
  }
  auto valid_numeric = [](ValueType::Type type) {
    return type == ValueType::Type::Int || type == ValueType::Type::Double;
  };
  auto left = left_type->GetType();
  auto right = right_type->GetType();
  if (!valid_numeric(left) || !valid_numeric(right)) {
    message =
        "arithmetic just support INT/DOUBLE, please CAST other types first";
    return nullptr;
  }
  if (left == ValueType::Type::Double || right == ValueType::Type::Double) {
    return std::make_shared<Double>();
  }
  return std::make_shared<Int>();
}

BoundExpressRef Transform::CreateArithmeticFunction(TokenType op_type,
                                                    BoundExpressRef lhs,
                                                    BoundExpressRef rhs,
                                                    std::string &message) {
  auto result_type = DeduceNumericType(lhs, rhs, message);
  if (!message.empty() || result_type == nullptr) {
    return nullptr;
  }
  auto make_args = [&]() {
    std::vector<BoundExpressRef> args;
    args.emplace_back(lhs);
    args.emplace_back(rhs);
    return args;
  };
  using Operator = FunctionBinaryArithmetic::Operator;
  switch (op_type) {
  case TokenType::Plus:
    return std::make_shared<BoundFunction>(
        std::make_shared<FunctionBinaryArithmetic>(Operator::Add, result_type),
        make_args());
  case TokenType::Minus:
    return std::make_shared<BoundFunction>(
        std::make_shared<FunctionBinaryArithmetic>(Operator::Sub, result_type),
        make_args());
  case TokenType::Asterisk:
    return std::make_shared<BoundFunction>(
        std::make_shared<FunctionBinaryArithmetic>(Operator::Mul, result_type),
        make_args());
  case TokenType::Slash:
    return std::make_shared<BoundFunction>(
        std::make_shared<FunctionBinaryArithmetic>(Operator::Div, result_type),
        make_args());
  default:
    message = fmt::format("operator {} not supported", getTokenName(op_type));
  }
  return nullptr;
}

BoundExpressRef Transform::ParsePrimary(TokenIterator &it, TokenIterator end,
                                        std::vector<TableMetaRef> &tables,
                                        std::vector<BoundExpressRef> &columns,
                                        std::string &message) {
  switch (it->type) {
  case TokenType::OpeningRoundBracket: {
    auto begin = it;
    if (!(++begin < end)) {
      message = "expect expression after (";
      return nullptr;
    }
    it = begin;
    auto expr = ParseExpression(it, end, tables, columns, message);
    if (!expr || !message.empty()) {
      return expr;
    }
    auto closing = it;
    if (!(++closing < end) || closing->type != TokenType::ClosingRoundBracket) {
      message = "expect )";
      return nullptr;
    }
    it = closing;
    return expr;
  }
  case TokenType::Number: return MakeNumericConstant(*it, false);
  case TokenType::StringLiteral: return MakeStringConstant(*it);
  case TokenType::BareWord:
    return ParseIdentifier(it, end, tables, columns, message);
  default: break;
  }
  message = fmt::format("unexpected token {}", getTokenName(it->type));
  return nullptr;
}

BoundExpressRef Transform::ParseUnary(TokenIterator &it, TokenIterator end,
                                      std::vector<TableMetaRef> &tables,
                                      std::vector<BoundExpressRef> &columns,
                                      std::string &message) {
  if (it->type == TokenType::Plus || it->type == TokenType::Minus) {
    auto op = it->type;
    auto next = it;
    if (!(++next < end)) {
      message = fmt::format("expect expression after {}", getTokenName(op));
      return nullptr;
    }
    it = next;
    auto operand = ParseUnary(it, end, tables, columns, message);
    if (!operand || !message.empty()) {
      return operand;
    }
    if (op == TokenType::Plus) {
      return operand;
    }
    auto zero = MakeZeroConstant();
    return CreateArithmeticFunction(TokenType::Minus, zero, operand, message);
  }
  return ParsePrimary(it, end, tables, columns, message);
}

BoundExpressRef
Transform::ParseExpression(TokenIterator &it, TokenIterator end,
                           std::vector<TableMetaRef> &tables,
                           std::vector<BoundExpressRef> &columns,
                           std::string &message, int min_precedence) {
  auto left = ParseUnary(it, end, tables, columns, message);
  if (!left || !message.empty()) {
    return left;
  }
  while (true) {
    auto lookahead = it;
    ++lookahead;
    if (!(lookahead < end)) {
      break;
    }
    int precedence = GetBinaryPrecedence(lookahead->type);
    if (precedence < min_precedence) {
      break;
    }
    TokenType op_type = lookahead->type;
    it = lookahead;
    auto rhs_token = it;
    if (!(++rhs_token < end)) {
      message = fmt::format("operator {} miss rhs", getTokenName(op_type));
      return nullptr;
    }
    it = rhs_token;
    auto right =
        ParseExpression(it, end, tables, columns, message, precedence + 1);
    if (!right || !message.empty()) {
      return nullptr;
    }
    left = CreateArithmeticFunction(op_type, left, right, message);
    if (!left || !message.empty()) {
      return nullptr;
    }
  }
  return left;
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
    auto next = it;
    ++next;
    if (next < end && next->type != TokenType::Comma &&
        next->type != TokenType::ClosingRoundBracket) {
      message = "* just support standalone usage";
    }
    return nullptr;
  }
  return ParseExpression(it, end, tables, columns, message);
}
} // namespace DB
