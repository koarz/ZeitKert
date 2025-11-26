#include "function/FunctionCast.hpp"
#include "common/Status.hpp"
#include "common/util/StringUtil.hpp"
#include "fmt/format.h"
#include "storage/Block.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/Double.hpp"
#include "type/Int.hpp"
#include "type/String.hpp"

#include <algorithm>
#include <memory>
#include <string>

namespace DB {

static std::shared_ptr<ValueType> MakeCastTarget(std::string type_name) {
  StringUtil::ToUpper(type_name);
  if (type_name == "INT") {
    return std::make_shared<Int>();
  }
  if (type_name == "DOUBLE") {
    return std::make_shared<Double>();
  }
  if (type_name == "STRING") {
    return std::make_shared<String>();
  }
  return nullptr;
}

Status
FunctionCast::ResolveResultType(Block &block,
                                std::shared_ptr<ValueType> &result_type) const {
  if (block.Size() < 2) {
    return Status::Error(
        ErrorCode::BindError,
        "CAST function requires 2 arguments: cast(value, 'type_name')");
  }

  auto type_column = block.GetColumn(1);
  if (type_column->GetValueType()->GetType() != ValueType::Type::String) {
    return Status::Error(ErrorCode::BindError,
                         "CAST second argument must be a string type name");
  }

  std::string type_name = type_column->GetStrElement(0);
  if (type_name.size() >= 2 && type_name.front() == '\'' &&
      type_name.back() == '\'') {
    type_name = type_name.substr(1, type_name.size() - 2);
  }

  result_type = MakeCastTarget(type_name);
  if (result_type == nullptr) {
    return Status::Error(
        ErrorCode::BindError,
        fmt::format("Unknown type name '{}' for CAST", type_name));
  }

  return Status::OK();
}

static size_t CastEffectiveIndex(const ColumnWithNameTypeRef &column,
                                 size_t row) {
  auto size = column->Size();
  if (size == 0 || size == 1) {
    return 0;
  }
  return std::min(row, size - 1);
}

static std::string ReadAsString(const ColumnWithNameTypeRef &column,
                                size_t row) {
  auto idx = CastEffectiveIndex(column, row);
  return column->GetStrElement(idx);
}

static bool TryReadInt(const ColumnWithNameTypeRef &column, size_t row,
                       int &value, std::string &error) {
  auto idx = CastEffectiveIndex(column, row);
  switch (column->GetValueType()->GetType()) {
  case ValueType::Type::Int: {
    auto &col = static_cast<ColumnVector<int> &>(*column->GetColumn());
    value = col[idx];
    return true;
  }
  case ValueType::Type::Double: {
    auto &col = static_cast<ColumnVector<double> &>(*column->GetColumn());
    value = static_cast<int>(col[idx]);
    return true;
  }
  case ValueType::Type::String: {
    auto raw = column->GetStrElement(idx);
    try {
      value = std::stoi(raw);
      return true;
    } catch (...) {
      error = fmt::format("cannot cast {} to INT", raw);
    }
    break;
  }
  default: break;
  }
  if (error.empty()) {
    error = "cast only support INT/DOUBLE/STRING sources";
  }
  return false;
}

static bool TryReadDouble(const ColumnWithNameTypeRef &column, size_t row,
                          double &value, std::string &error) {
  auto idx = CastEffectiveIndex(column, row);
  switch (column->GetValueType()->GetType()) {
  case ValueType::Type::Int: {
    auto &col = static_cast<ColumnVector<int> &>(*column->GetColumn());
    value = static_cast<double>(col[idx]);
    return true;
  }
  case ValueType::Type::Double: {
    auto &col = static_cast<ColumnVector<double> &>(*column->GetColumn());
    value = col[idx];
    return true;
  }
  case ValueType::Type::String: {
    auto raw = column->GetStrElement(idx);
    try {
      value = std::stod(raw);
      return true;
    } catch (...) {
      error = fmt::format("cannot cast {} to DOUBLE", raw);
    }
    break;
  }
  default: break;
  }
  if (error.empty()) {
    error = "cast only support INT/DOUBLE/STRING sources";
  }
  return false;
}

Status FunctionCast::ExecuteImpl(Block &block, size_t result_idx,
                                 size_t input_rows_count) const {
  if (block.Size() < 2) {
    return Status::Error(ErrorCode::BindError, "CAST need 2 arguments");
  }

  auto type_column = block.GetColumn(1);
  std::string type_name = type_column->GetStrElement(0);
  if (type_name.size() >= 2 && type_name.front() == '\'' &&
      type_name.back() == '\'') {
    type_name = type_name.substr(1, type_name.size() - 2);
  }
  auto target_type = MakeCastTarget(type_name);

  if (target_type == nullptr) {
    return Status::Error(ErrorCode::BindError, "CAST target type not resolved");
  }

  auto input = block.GetColumn(0);
  auto result_column = block.GetColumn(result_idx)->GetColumn();

  switch (target_type->GetType()) {
  case ValueType::Type::Int: {
    auto &res = static_cast<ColumnVector<int> &>(*result_column);
    for (size_t row = 0; row < input_rows_count; ++row) {
      int value{};
      std::string error;
      if (!TryReadInt(input, row, value, error)) {
        return Status::Error(ErrorCode::BindError, error);
      }
      res.Insert(value);
    }
    break;
  }
  case ValueType::Type::Double: {
    auto &res = static_cast<ColumnVector<double> &>(*result_column);
    for (size_t row = 0; row < input_rows_count; ++row) {
      double value{};
      std::string error;
      if (!TryReadDouble(input, row, value, error)) {
        return Status::Error(ErrorCode::BindError, error);
      }
      res.Insert(value);
    }
    break;
  }
  case ValueType::Type::String: {
    auto &res = static_cast<ColumnString &>(*result_column);
    for (size_t row = 0; row < input_rows_count; ++row) {
      auto value = ReadAsString(input, row);
      res.Insert(std::move(value));
    }
    break;
  }
  case ValueType::Type::Null:
    return Status::Error(ErrorCode::BindError,
                         "cast target type not supported");
  }
  return Status::OK();
}
} // namespace DB
