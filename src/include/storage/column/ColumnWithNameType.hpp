#pragma once

#include "storage/column/Column.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <memory>
#include <string>

namespace DB {
class TableMeta;

class ColumnWithNameType {
  friend TableMeta;
  ColumnPtr column_;

  std::string name_;
  std::shared_ptr<ValueType> type_;

public:
  explicit ColumnWithNameType(ColumnPtr column, std::string name,
                              std::shared_ptr<ValueType> type)
      : column_(column), name_(name), type_(type) {}

  std::string GetColumnName() { return name_; }

  std::string GetStrElement(size_t idx) { return column_->GetStrElement(idx); }

  std::shared_ptr<ValueType> GetValueType() { return type_; }

  ColumnPtr GetColumn() { return column_; }

  size_t Size() { return column_->Size(); }
};
using ColumnWithNameTypeRef = std::shared_ptr<ColumnWithNameType>;
} // namespace DB