#include "function/Abs.hpp"
#include "storage/column/ColumnVector.hpp"

namespace DB {
Status FunctionAbs::ExecuteImpl(Block &block, size_t result_idx,
                                size_t input_rows_count) const {
  auto &arg =
      dynamic_cast<ColumnVector<int> &>(*block.GetColumn(0)->GetColumn());
  auto &res = dynamic_cast<ColumnVector<int> &>(
      *block.GetColumn(result_idx)->GetColumn());

  for (size_t i = 0; i < input_rows_count; i++) {
    res.Insert(std::abs(arg[i]));
  }
  return Status::OK();
}

} // namespace DB