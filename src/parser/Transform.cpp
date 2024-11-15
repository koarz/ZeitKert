#include "parser/Transform.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/Checker.hpp"
#include "parser/Lexer.hpp"
#include "parser/TokenIterator.hpp"
#include "parser/binder/BoundConstant.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "type/Double.hpp"
#include "type/Int.hpp"
#include "type/String.hpp"

#include <memory>
#include <string>
#include <vector>

namespace DB {
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