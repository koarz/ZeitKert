#pragma once

#include "catalog/Schema.hpp"

namespace DB {
class ResultSet {
public:
  SchemaRef schema_;
};
} // namespace DB