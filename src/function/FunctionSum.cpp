#include "function/FunctionSum.hpp"
#include "storage/Block.hpp"
#include "storage/column/ColumnVector.hpp"
#include "type/Double.hpp"
#include "type/Int.hpp"

namespace DB {
Status
FunctionSum::ResolveResultType(Block &block,
                               std::shared_ptr<ValueType> &result_type) const {
  if (block.Size() != 1) {
    return Status::Error(ErrorCode::BindError,
                         "SUM requires exactly 1 argument");
  }
  auto input_type = block.GetColumn(0)->GetValueType()->GetType();
  if (input_type != ValueType::Type::Int &&
      input_type != ValueType::Type::Double) {
    return Status::Error(ErrorCode::BindError,
                         "SUM only supports INT or DOUBLE columns");
  }
  // SUM 返回 Double 以避免溢出
  result_type = std::make_shared<Double>();
  return Status::OK();
}

Status FunctionSum::ExecuteImpl(Block &block, size_t result_idx,
                                size_t input_rows_count) const {
  if (result_idx != 1) {
    return Status::Error(ErrorCode::BindError,
                         "SUM requires exactly 1 argument");
  }

  auto input_col = block.GetColumn(0);
  auto input_type = input_col->GetValueType()->GetType();
  double sum = 0.0;

  switch (input_type) {
  case ValueType::Type::Int: {
    auto &col = static_cast<ColumnVector<int> &>(*input_col->GetColumn());
    for (size_t i = 0; i < input_rows_count; i++) {
      sum += col[i];
    }
    break;
  }
  case ValueType::Type::Double: {
    auto &col = static_cast<ColumnVector<double> &>(*input_col->GetColumn());
    for (size_t i = 0; i < input_rows_count; i++) {
      sum += col[i];
    }
    break;
  }
  default:
    return Status::Error(ErrorCode::BindError,
                         "SUM only supports INT or DOUBLE columns");
  }

  auto &res = static_cast<ColumnVector<double> &>(
      *block.GetColumn(result_idx)->GetColumn());
  res.Insert(sum);
  return Status::OK();
}
} // namespace DB
