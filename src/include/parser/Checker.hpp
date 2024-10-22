#pragma once

#include "common/Instance.hpp"
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

  bool IsKeyWord(std::string_view keyword) { return keywords_.Exist(keyword); }

  bool IsFunction(std::string_view function) {
    return functions_.Exist(function);
  }

  std::shared_ptr<Function> GetFuncImpl(std::string function) {
    return func_impl_[function];
  }
};
} // namespace DB