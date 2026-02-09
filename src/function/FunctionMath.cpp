#include "function/FunctionMath.hpp"
#include "fmt/format.h"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/ValueType.hpp"

#include <algorithm>
#include <cmath>

namespace DB {

static size_t MathEffectiveIndex(const ColumnWithNameTypeRef &column,
                                 size_t row) {
  auto size = column->Size();
  if (size == 0 || size == 1) {
    return 0;
  }
  return std::min(row, size - 1);
}

static bool MathTryGetNumeric(const ColumnWithNameTypeRef &column, size_t row,
                              double &value) {
  auto idx = MathEffectiveIndex(column, row);
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

static double ApplyMathOp(FunctionMath::MathOp op, double v) {
  switch (op) {
  case FunctionMath::MathOp::Sqrt:  return std::sqrt(v);
  case FunctionMath::MathOp::Sin:   return std::sin(v);
  case FunctionMath::MathOp::Cos:   return std::cos(v);
  case FunctionMath::MathOp::Tan:   return std::tan(v);
  case FunctionMath::MathOp::Asin:  return std::asin(v);
  case FunctionMath::MathOp::Acos:  return std::acos(v);
  case FunctionMath::MathOp::Atan:  return std::atan(v);
  case FunctionMath::MathOp::Log:   return std::log(v);
  case FunctionMath::MathOp::Log10: return std::log10(v);
  case FunctionMath::MathOp::Exp:   return std::exp(v);
  case FunctionMath::MathOp::Ceil:  return std::ceil(v);
  case FunctionMath::MathOp::Floor: return std::floor(v);
  case FunctionMath::MathOp::Round: return std::round(v);
  }
  return 0.0;
}

static std::string MathOpName(FunctionMath::MathOp op) {
  switch (op) {
  case FunctionMath::MathOp::Sqrt:  return "SQRT";
  case FunctionMath::MathOp::Sin:   return "SIN";
  case FunctionMath::MathOp::Cos:   return "COS";
  case FunctionMath::MathOp::Tan:   return "TAN";
  case FunctionMath::MathOp::Asin:  return "ASIN";
  case FunctionMath::MathOp::Acos:  return "ACOS";
  case FunctionMath::MathOp::Atan:  return "ATAN";
  case FunctionMath::MathOp::Log:   return "LOG";
  case FunctionMath::MathOp::Log10: return "LOG10";
  case FunctionMath::MathOp::Exp:   return "EXP";
  case FunctionMath::MathOp::Ceil:  return "CEIL";
  case FunctionMath::MathOp::Floor: return "FLOOR";
  case FunctionMath::MathOp::Round: return "ROUND";
  }
  return "MATH";
}

FunctionMath::FunctionMath(MathOp op) : name_(MathOpName(op)), op_(op) {}

Status FunctionMath::ExecuteImpl(Block &block, size_t result_idx,
                                 size_t input_rows_count) const {
  auto arg = block.GetColumn(0);
  auto &res = static_cast<ColumnVector<double> &>(
      *block.GetColumn(result_idx)->GetColumn());

  for (size_t row = 0; row < input_rows_count; ++row) {
    double value{};
    if (!MathTryGetNumeric(arg, row, value)) {
      return Status::Error(
          ErrorCode::BindError,
          fmt::format("{} only supports INT/DOUBLE arguments", name_));
    }
    res.Insert(ApplyMathOp(op_, value));
  }
  return Status::OK();
}
} // namespace DB
