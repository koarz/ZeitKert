#include "execution/ValuesExecutor.hpp"
#include "common/Status.hpp"
#include "parser/binder/BoundConstant.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"

#include <memory>

namespace DB {

Status ValuesExecutor::Init() {
  emitted_ = false;
  return Status::OK();
}

Status ValuesExecutor::Next(DataChunk &out) {
  // 一次性发射所有字面量；之后返回空 chunk 表示 EOF
  if (emitted_) {
    return Status::OK();
  }

  for (auto &v : plan_->values_) {
    auto &value = static_cast<BoundConstant &>(*v);
    ColumnPtr col;
    switch (value.type_->GetType()) {
    case ValueType::Type::Int:
      col = std::make_shared<ColumnVector<int>>();
      static_cast<ColumnVector<int> &>(*col).Insert(value.value_.i32);
      break;
    case ValueType::Type::String:
      col = std::make_shared<ColumnString>();
      static_cast<ColumnString &>(*col).Insert(value.ToString());
      break;
    case ValueType::Type::Double:
      col = std::make_shared<ColumnVector<double>>();
      static_cast<ColumnVector<double> &>(*col).Insert(value.value_.f64);
      break;
    case ValueType::Type::Null:
      // NULL 字面量当作 1 行 0 值并设置 null bitmap
      col = std::make_shared<ColumnVector<int>>();
      static_cast<ColumnVector<int> &>(*col).Insert(0);
      col->SetNull(0);
      break;
    }

    auto col_name = value.ToString();
    auto named =
        std::make_shared<ColumnWithNameType>(col, col_name, value.type_);
    out.AddColumn(named);
  }

  emitted_ = true;
  return Status::OK();
}

} // namespace DB
