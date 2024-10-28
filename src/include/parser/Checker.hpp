#pragma once

#include "common/util/StringUtil.hpp"
#include "parser/Function.hpp"
#include "storage/Trie.hpp"

#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace DB {
// The Checker have some trie that store registered keywords and function
class Checker {
  static Trie types_;
  static Trie keywords_;
  static Trie functions_;

  static std::map<std::string, std::shared_ptr<Function>> func_impl_;

public:
  static void RegisterType(std::string_view type);

  static void RegisterKeyWord(std::string_view keyword);

  static void RegisterFunction(std::string_view function,
                               std::shared_ptr<Function> func_impl);

  static bool IsType(std::string &str);

  static bool IsKeyWord(std::string &str);

  static bool IsFunction(std::string_view src, std::shared_ptr<Function> func);

  static std::shared_ptr<Function> GetFuncImpl(std::string function);
};
} // namespace DB