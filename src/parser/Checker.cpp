#include "parser/Checker.hpp"
#include "common/util/StringUtil.hpp"

namespace DB {
// static member need define on cpp file
Trie Checker::types_;
Trie Checker::keywords_;
Trie Checker::functions_;
std::map<std::string, std::shared_ptr<Function>> Checker::func_impl_;

void Checker::RegisterType(std::string_view type) {
  types_.Insert(type);
}

void Checker::RegisterKeyWord(std::string_view keyword) {
  keywords_.Insert(keyword);
}

void Checker::RegisterFunction(std::string_view function,
                               std::shared_ptr<Function> func_impl) {
  functions_.Insert(function);
  func_impl_.emplace(function, func_impl);
}

bool Checker::IsType(std::string &str) {
  StringUtil::ToUpper(str);
  return types_.Exist(str);
}

bool Checker::IsKeyWord(std::string &str) {
  StringUtil::ToUpper(str);
  return keywords_.Exist(str);
}

bool Checker::IsFunction(std::string &str) {
  StringUtil::ToUpper(str);
  return functions_.Exist(str);
}

std::shared_ptr<Function> Checker::GetFuncImpl(std::string function) {
  return func_impl_[function];
}
} // namespace DB