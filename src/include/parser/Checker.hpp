#pragma once

#include "common/Instance.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/Function.hpp"
#include "storage/Trie.hpp"

#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace DB {
// The Checker have some trie that store registered keywords and function
class Checker : public Instance<Checker> {
  Trie keywords_;
  Trie functions_;

  std::map<std::string, std::shared_ptr<Function>> func_impl_;

public:
  void RegisterKeyWord(std::string_view keyword) { keywords_.Insert(keyword); }

  void RegisterFunction(std::string_view function,
                        std::shared_ptr<Function> func_impl) {
    functions_.Insert(function);
    func_impl_.emplace(function, func_impl);
  }

  bool IsKeyWord(std::string &str) {
    StringUtil::ToUpper(str);
    return keywords_.Exist(str);
  }

  bool IsFunction(std::string_view src, std::shared_ptr<Function> func) {
    std::string str{src};
    StringUtil::ToUpper(str);
    if (functions_.Exist(str)) {
      func = func_impl_[str];
      return true;
    }
    return false;
  }

  std::shared_ptr<Function> GetFuncImpl(std::string function) {
    return func_impl_[function];
  }
};
} // namespace DB