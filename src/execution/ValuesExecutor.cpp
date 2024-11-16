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
  return Status::OK();
}

Status ValuesExecutor::Execute() {
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
    case ValueType::Type::Null: break;
    }
    auto col_name = value.ToString();
    auto column =
        std::make_shared<ColumnWithNameType>(col, col_name, value.type_);
    schema_->GetColumns().push_back(column);
  }
  return Status::OK();
}
} // namespace DB