#pragma once

#include "type/ValueType.hpp"

#include <memory>

namespace DB {
class Column {

public:
  virtual bool IsConstColumn() { return false; }
  virtual bool IsNullable() { return true; }
};
using ColumnPtr = std::shared_ptr<Column>;
} // namespace DB