#pragma once

#include "parser/binder/BoundExpress.hpp"

namespace DB {
class BoundColumnRef : public BoundExpress {
  std::string column_name_;

public:
  BoundColumnRef(std::string column_name)
      : BoundExpress(BoundExpressType::BoundColumnRef),
        column_name_(std::move(column_name)) {}
  ~BoundColumnRef() override = default;

  std::string &GetColumnName() { return column_name_; }
};
} // namespace DB