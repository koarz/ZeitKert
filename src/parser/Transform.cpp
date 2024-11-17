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

#include <memory>
#include <string>
#include <vector>

namespace DB {
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
  if (!(it < end)) {
    return nullptr;
  }
  if (it->type == TokenType::Comma) {
    ++it;
    return GetColumnExpress(it, end, tables, columns, message);
  }

  if (it->type == TokenType::Asterisk) {
    for (auto &table : tables) {
      for (auto &col_meta : table->GetColumns()) {
        columns.push_back(std::make_shared<BoundColumnMeta>(col_meta));
      }
    }
    return nullptr;
  }

  if (it->type == TokenType::Minus) {
    if ((++it)->type == TokenType::Number) {
      auto number = std::make_shared<BoundConstant>();
      std::string numstr{it->begin, it->end};
      if (StringUtil::IsInteger(numstr)) {
        numstr = "-" + numstr;
        number->type_ = std::make_shared<Int>();
        number->value_.i32 = std::stoi(numstr);
      } else {
        numstr = "-" + numstr;
        number->type_ = std::make_shared<Double>();
        number->value_.f64 = std::stod(numstr);
      }
      return number;
    }
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
      while (it < end && it->type != TokenType::ClosingRoundBracket) {
        auto arg = GetColumnExpress(++it, end, tables, arguments, message);
        if (!message.empty()) {
          return nullptr;
        }
        if (arg) {
          arguments.push_back(arg);
        }
      }
      return std::make_shared<BoundFunction>(Checker::GetFuncImpl(func_name),
                                             std::move(arguments));
    }
    if (it == end) {}
    // check is table.col ?
    std::string col_name;
    if (it->type == TokenType::Dot) {
      ++it;
      col_name = {temp_begin->begin, it->end};
    } else {
      col_name = {temp_begin->begin, temp_begin->end};
    }

    std::string table_name, column_name;
    switch (StringUtil::SplitTableColumn(col_name, table_name, column_name)) {
    case -1: {
      message = fmt::format("your column: {} not correct", col_name);
      return nullptr;
    }
    case 0: {
      for (auto &table : tables) {
        columns.push_back(
            std::make_shared<BoundColumnMeta>(table->GetColumn(column_name)));
      }
      break;
    }
    case 1: {
      for (auto &table : tables) {
        if (table->GetTableName() == table_name) {
          columns.push_back(
              std::make_shared<BoundColumnMeta>(table->GetColumn(column_name)));
          break;
        }
      }
      break;
    }
    }
  }
  return nullptr;
}
} // namespace DB