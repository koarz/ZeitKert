#pragma once

#include "type/ValueType.hpp"

#include <memory>

namespace DB {
class Column {

public:
  virtual bool IsConstColumn() { return false; }
  virtual bool IsNullable() { return true; }
  virtual std::string GetStrElement(size_t idx) = 0;
  virtual size_t Size() = 0;
  virtual size_t GetMaxElementSize() = 0;
};
using ColumnPtr = std::shared_ptr<Column>;
} // namespace DB