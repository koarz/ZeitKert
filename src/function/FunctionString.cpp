#include "function/FunctionString.hpp"
#include "common/util/StringUtil.hpp"
#include "storage/column/ColumnString.hpp"

#include <cctype>

namespace DB {
Status FunctionToLower::ExecuteImpl(Block &block, size_t result_idx,
                                    size_t input_rows_count) const {
  auto &arg = static_cast<ColumnString &>(*block.GetColumn(0)->GetColumn());
  auto &res =
      static_cast<ColumnString &>(*block.GetColumn(result_idx)->GetColumn());

  for (size_t i = 0; i < input_rows_count; i++) {
    auto s = arg[i];
    StringUtil::ToLower(s);
    res.Insert(std::move(s));
  }
  return Status::OK();
}

Status FunctionToUpper::ExecuteImpl(Block &block, size_t result_idx,
                                    size_t input_rows_count) const {
  auto &arg = static_cast<ColumnString &>(*block.GetColumn(0)->GetColumn());
  auto &res =
      static_cast<ColumnString &>(*block.GetColumn(result_idx)->GetColumn());

  for (size_t i = 0; i < input_rows_count; i++) {
    auto s = arg[i];
    StringUtil::ToUpper(s);
    res.Insert(std::move(s));
  }
  return Status::OK();
}
} // namespace DB