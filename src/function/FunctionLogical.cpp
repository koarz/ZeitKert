#include "function/FunctionLogical.hpp"
#include "common/Status.hpp"
#include "fmt/format.h"
#include "storage/Block.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/ValueType.hpp"

#include <algorithm>

namespace DB {

static size_t LogicalEffectiveIndex(const ColumnWithNameTypeRef &column,
                                    size_t row) {
  auto size = column->Size();
  if (size == 0 || size == 1) {
    return 0;
  }
  return std::min(row, size - 1);
}

static std::string OperatorName(FunctionLogical::Operator op) {
  switch (op) {
  case FunctionLogical::Operator::And: return "AND";
  case FunctionLogical::Operator::Or: return "OR";
  }
  return "LOGICAL";
}

FunctionLogical::FunctionLogical(Operator op)
    : name_(OperatorName(op)), op_(op) {}

Status FunctionLogical::ExecuteImpl(Block &block, size_t result_idx,
                                    size_t input_rows_count) const {
  if (block.Size() < 2) {
    return Status::Error(ErrorCode::BindError,
                         fmt::format("{} need two arguments", name_));
  }
  auto lhs = block.GetColumn(0);
  auto rhs = block.GetColumn(1);
  auto result_column = block.GetColumn(result_idx)->GetColumn();
  auto &int_res = static_cast<ColumnVector<int> &>(*result_column);

  // 逻辑运算只接受 Int 类型（0 或非 0）
  auto lhs_type = lhs->GetValueType()->GetType();
  auto rhs_type = rhs->GetValueType()->GetType();

  if (lhs_type != ValueType::Type::Int || rhs_type != ValueType::Type::Int) {
    return Status::Error(ErrorCode::BindError,
                         "logical operators require integer operands");
  }

  auto &lhs_col = static_cast<ColumnVector<int> &>(*lhs->GetColumn());
  auto &rhs_col = static_cast<ColumnVector<int> &>(*rhs->GetColumn());

  for (size_t row = 0; row < input_rows_count; ++row) {
    auto lhs_idx = LogicalEffectiveIndex(lhs, row);
    auto rhs_idx = LogicalEffectiveIndex(rhs, row);

    int lhs_value = lhs_col[lhs_idx];
    int rhs_value = rhs_col[rhs_idx];
    int result = 0;

    switch (op_) {
    case Operator::And:
      result = (lhs_value != 0 && rhs_value != 0) ? 1 : 0;
      break;
    case Operator::Or:
      result = (lhs_value != 0 || rhs_value != 0) ? 1 : 0;
      break;
    }

    int_res.Insert(result);
  }
  return Status::OK();
}
} // namespace DB
