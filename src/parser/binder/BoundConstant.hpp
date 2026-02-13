#pragma once

#include "parser/binder/BoundExpress.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <memory>
#include <string>

namespace DB {
struct BoundConstant : public BoundExpress {
  BoundConstant() : BoundExpress(BoundExpressType::BoundConstant) {}
  ~BoundConstant() override = default;

  union {
    int i32;
    char *str;
    double f64;
  } value_;

  size_t size_;

  std::shared_ptr<ValueType> type_;

  std::string ToString() {
    switch (type_->GetType()) {

    case ValueType::Type::Int: return std::to_string(value_.i32);
    case ValueType::Type::String: return {value_.str, size_};
    case ValueType::Type::Double: return std::to_string(value_.f64);
    case ValueType::Type::Null: return "Null";
    }
    return "_unknow_";
  }
};
} // namespace DB