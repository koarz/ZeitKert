#include "function/FunctionComparison.hpp"
#include "common/Status.hpp"
#include "fmt/format.h"
#include "storage/Block.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/ValueType.hpp"

#include <algorithm>
#include <cstring>

namespace DB {

static size_t ComparisonEffectiveIndex(const ColumnWithNameTypeRef &column,
                                       size_t row) {
  auto size = column->Size();
  if (size == 0 || size == 1) {
    return 0;
  }
  return std::min(row, size - 1);
}

static std::string OperatorName(FunctionComparison::Operator op) {
  switch (op) {
  case FunctionComparison::Operator::Less: return "LT";
  case FunctionComparison::Operator::LessOrEquals: return "LE";
  case FunctionComparison::Operator::Greater: return "GT";
  case FunctionComparison::Operator::GreaterOrEquals: return "GE";
  case FunctionComparison::Operator::Equals: return "EQ";
  case FunctionComparison::Operator::NotEquals: return "NE";
  }
  return "CMP";
}

FunctionComparison::FunctionComparison(Operator op)
    : name_(OperatorName(op)), op_(op) {}

Status FunctionComparison::ExecuteImpl(Block &block, size_t result_idx,
                                       size_t input_rows_count) const {
  if (block.Size() < 2) {
    return Status::Error(ErrorCode::BindError,
                         fmt::format("{} need two arguments", name_));
  }
  auto lhs = block.GetColumn(0);
  auto rhs = block.GetColumn(1);
  auto result_column = block.GetColumn(result_idx)->GetColumn();
  auto &int_res = static_cast<ColumnVector<int> &>(*result_column);

  auto lhs_type = lhs->GetValueType()->GetType();
  auto rhs_type = rhs->GetValueType()->GetType();

  for (size_t row = 0; row < input_rows_count; ++row) {
    auto lhs_idx = ComparisonEffectiveIndex(lhs, row);
    auto rhs_idx = ComparisonEffectiveIndex(rhs, row);

    int result = 0;

    // 数值比较
    if ((lhs_type == ValueType::Type::Int || lhs_type == ValueType::Type::Double) &&
        (rhs_type == ValueType::Type::Int || rhs_type == ValueType::Type::Double)) {
      double lhs_value = 0.0, rhs_value = 0.0;

      if (lhs_type == ValueType::Type::Int) {
        lhs_value = static_cast<ColumnVector<int> &>(*lhs->GetColumn())[lhs_idx];
      } else {
        lhs_value = static_cast<ColumnVector<double> &>(*lhs->GetColumn())[lhs_idx];
      }

      if (rhs_type == ValueType::Type::Int) {
        rhs_value = static_cast<ColumnVector<int> &>(*rhs->GetColumn())[rhs_idx];
      } else {
        rhs_value = static_cast<ColumnVector<double> &>(*rhs->GetColumn())[rhs_idx];
      }

      switch (op_) {
      case Operator::Less: result = lhs_value < rhs_value ? 1 : 0; break;
      case Operator::LessOrEquals: result = lhs_value <= rhs_value ? 1 : 0; break;
      case Operator::Greater: result = lhs_value > rhs_value ? 1 : 0; break;
      case Operator::GreaterOrEquals: result = lhs_value >= rhs_value ? 1 : 0; break;
      case Operator::Equals: result = lhs_value == rhs_value ? 1 : 0; break;
      case Operator::NotEquals: result = lhs_value != rhs_value ? 1 : 0; break;
      }
    }
    // 字符串比较
    else if (lhs_type == ValueType::Type::String &&
             rhs_type == ValueType::Type::String) {
      auto &lhs_col = static_cast<ColumnString &>(*lhs->GetColumn());
      auto &rhs_col = static_cast<ColumnString &>(*rhs->GetColumn());
      auto lhs_str = lhs_col[lhs_idx];
      auto rhs_str = rhs_col[rhs_idx];
      int cmp = lhs_str.compare(rhs_str);

      switch (op_) {
      case Operator::Less: result = cmp < 0 ? 1 : 0; break;
      case Operator::LessOrEquals: result = cmp <= 0 ? 1 : 0; break;
      case Operator::Greater: result = cmp > 0 ? 1 : 0; break;
      case Operator::GreaterOrEquals: result = cmp >= 0 ? 1 : 0; break;
      case Operator::Equals: result = cmp == 0 ? 1 : 0; break;
      case Operator::NotEquals: result = cmp != 0 ? 1 : 0; break;
      }
    } else {
      return Status::Error(ErrorCode::BindError,
                           "comparison between incompatible types");
    }

    int_res.Insert(result);
  }
  return Status::OK();
}
} // namespace DB
