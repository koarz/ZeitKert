#include "function/FunctionArithmetic.hpp"
#include "common/Status.hpp"
#include "fmt/format.h"
#include "storage/Block.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/ValueType.hpp"

#include <algorithm>
#include <cmath>

namespace DB {

static size_t ArithmeticEffectiveIndex(const ColumnWithNameTypeRef &column,
                                       size_t row) {
  auto size = column->Size();
  if (size == 0 || size == 1) {
    return 0;
  }
  return std::min(row, size - 1);
}

static bool ArithmeticTryGetNumeric(const ColumnWithNameTypeRef &column,
                                    size_t row, double &value) {
  auto idx = ArithmeticEffectiveIndex(column, row);
  switch (column->GetValueType()->GetType()) {
  case ValueType::Type::Int: {
    auto &col = static_cast<ColumnVector<int> &>(*column->GetColumn());
    value = col[idx];
    return true;
  }
  case ValueType::Type::Double: {
    auto &col = static_cast<ColumnVector<double> &>(*column->GetColumn());
    value = col[idx];
    return true;
  }
  default: break;
  }
  return false;
}

static double ApplyOperator(FunctionBinaryArithmetic::Operator op, double lhs,
                            double rhs) {
  switch (op) {
  case FunctionBinaryArithmetic::Operator::Add: return lhs + rhs;
  case FunctionBinaryArithmetic::Operator::Sub: return lhs - rhs;
  case FunctionBinaryArithmetic::Operator::Mul: return lhs * rhs;
  case FunctionBinaryArithmetic::Operator::Div: return lhs / rhs;
  }
  return 0.0;
}

static std::string OperatorName(FunctionBinaryArithmetic::Operator op) {
  switch (op) {
  case FunctionBinaryArithmetic::Operator::Add: return "ADD";
  case FunctionBinaryArithmetic::Operator::Sub: return "SUB";
  case FunctionBinaryArithmetic::Operator::Mul: return "MUL";
  case FunctionBinaryArithmetic::Operator::Div: return "DIV";
  }
  return "ARITH";
}

FunctionBinaryArithmetic::FunctionBinaryArithmetic(
    Operator op, std::shared_ptr<ValueType> result_type)
    : name_(OperatorName(op)), op_(op), result_type_(std::move(result_type)) {}

Status FunctionBinaryArithmetic::ExecuteImpl(Block &block, size_t result_idx,
                                             size_t input_rows_count) const {
  if (block.Size() < 2) {
    return Status::Error(ErrorCode::BindError,
                         fmt::format("{} need two arguments", name_));
  }
  auto lhs = block.GetColumn(0);
  auto rhs = block.GetColumn(1);
  auto result_column = block.GetColumn(result_idx)->GetColumn();
  bool use_double = result_type_->GetType() == ValueType::Type::Double;
  auto *int_res =
      use_double ? nullptr : &static_cast<ColumnVector<int> &>(*result_column);
  auto *double_res = use_double
                         ? &static_cast<ColumnVector<double> &>(*result_column)
                         : nullptr;

  for (size_t row = 0; row < input_rows_count; ++row) {
    double lhs_value{}, rhs_value{};
    if (!ArithmeticTryGetNumeric(lhs, row, lhs_value) ||
        !ArithmeticTryGetNumeric(rhs, row, rhs_value)) {
      return Status::Error(
          ErrorCode::BindError,
          fmt::format("{} just support INT/DOUBLE arguments", name_));
    }
    if (op_ == Operator::Div && rhs_value == 0.0) {
      return Status::Error(ErrorCode::BindError, "Division by zero");
    }
    double result = ApplyOperator(op_, lhs_value, rhs_value);
    if (use_double) {
      double_res->Insert(result);
    } else {
      int_res->Insert(static_cast<int>(result));
    }
  }
  return Status::OK();
}
} // namespace DB
