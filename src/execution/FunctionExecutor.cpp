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
  emitted_ = false;
  for (auto &child : children_) {
    auto status = child->Init();
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status FunctionExecutor::Next(DataChunk &out) {
  // 函数算子是一次性发射：第二次起 Next 直接返回空 chunk 表示 EOF
  if (emitted_) {
    return Status::OK();
  }

  // 拉取所有子节点的数据，平铺到一个 Block 上下文中
  Block block;
  Status status;
  size_t input_rows_num = 0;
  size_t result_idx = 0;

  for (auto &child : children_) {
    DataChunk child_chunk;
    status = child->Next(child_chunk);
    if (!status.ok()) {
      return status;
    }
    for (auto &col : child_chunk.columns()) {
      input_rows_num = std::max(col->Size(), input_rows_num);
      block.PushColumn(col);
      result_idx++;
    }
  }

  // 解析函数返回类型
  std::shared_ptr<ValueType> res_type;
  status = function_->ResolveResultType(block, res_type);
  if (!status.ok()) {
    return status;
  }
  if (res_type == nullptr) {
    return Status::Error(ErrorCode::BindError,
                         "function result type cannot be null");
  }

  // 按返回类型构造结果列
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
    return Status::Error(ErrorCode::BindError, "function result type invalid");
  }

  // 拼出形如 sum(a, b) 的输出列名
  std::string func_name =
      function_->GetName() + "(" + block.GetColumn(0)->GetColumnName();
  for (int i = 1; i < block.Size(); i++) {
    func_name += ", " + block.GetColumn(i)->GetColumnName();
  }
  func_name += ")";

  block.PushColumn(
      std::make_shared<ColumnWithNameType>(res_data, func_name, res_type));

  // 真正执行函数计算（聚合或逐行向量化由各 Function 自己实现）
  status = function_->ExecuteImpl(block, result_idx, input_rows_num);
  if (!status.ok()) {
    return status;
  }

  // 把结果列以 chunk 形式向上发射
  out.AddColumn(block.GetColumn(result_idx));
  emitted_ = true;
  return Status::OK();
}

} // namespace DB
