#include "execution/FunctionExecutor.hpp"
#include "common/Status.hpp"
#include "storage/Block.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include <cstddef>
#include <memory>

namespace DB {
Status FunctionExecutor::Init() {
  Status status;
  for (auto &child : children_) {
    status = child->Init();
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status FunctionExecutor::Execute() {
  Block block;
  Status status;
  size_t input_rows_num{}, result_idx{};
  for (auto &child : this->children_) {
    status = child->Execute();
    if (!status.ok()) {
      return status;
    }
    for (auto col : child->GetSchema()->GetColumns()) {
      input_rows_num = std::max(col->Size(), input_rows_num);
      block.PushColumn(col);
      result_idx++;
    }
  }
  auto res_type = function_->GetResultType();
  ColumnPtr res_data;
  switch (res_type->GetType()) {
  case ValueType::Type::Int:
    res_data = std::make_shared<ColumnVector<int>>();
    break;
  case ValueType::Type::Double:
    res_data = std::make_shared<ColumnVector<double>>();
    break;
  case ValueType::Type::String:
    res_data = std::make_shared<ColumnString>();
    break;
  case ValueType::Type::Null:
  case ValueType::Type::Varchar: break;
  }
  std::string func_name =
      function_->GetName() + "(" + block.GetColumn(0)->GetColumnName();
  for (int i = 1; i < block.Size(); i++) {
    func_name += ", " + block.GetColumn(i)->GetColumnName();
  }
  func_name += ")";
  block.PushColumn(
      std::make_shared<ColumnWithNameType>(res_data, func_name, res_type));
  status = function_->ExecuteImpl(block, result_idx, input_rows_num);
  if (!status.ok()) {
    return status;
  }
  schema_->GetColumns().push_back(block.GetColumn(result_idx));
  return Status::OK();
}
} // namespace DB