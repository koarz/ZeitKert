#include "function/FunctionCount.hpp"
#include "storage/Block.hpp"
#include "storage/column/ColumnVector.hpp"

namespace DB {
Status FunctionCount::ResolveResultType(
    Block &block, std::shared_ptr<ValueType> &result_type) const {
  if (block.Size() != 1) {
    return Status::Error(
        ErrorCode::BindError,
        "COUNT requires exactly 1 argument (use a table column)");
  }
  result_type = GetResultType();
  return Status::OK();
}

Status FunctionCount::ExecuteImpl(Block &block, size_t result_idx,
                                  size_t input_rows_count) const {
  if (result_idx != 1) {
    return Status::Error(ErrorCode::BindError,
                         "COUNT requires exactly 1 argument");
  }
  auto &res = static_cast<ColumnVector<int> &>(
      *block.GetColumn(result_idx)->GetColumn());
  res.Insert(static_cast<int>(input_rows_count));
  return Status::OK();
}
} // namespace DB
