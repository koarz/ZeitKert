#pragma once

#include "common/Status.hpp"
#include "storage/Block.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <memory>
#include <string>
namespace DB {
class Function {
public:
  virtual ~Function() = default;

  // return function name
  virtual std::string GetName() const = 0;

  // execute the block store the data from executor
  // the result_idx is result should store which index in block
  // input row count is how many rows data have
  // the block's column only include arguments and result column
  virtual Status ExecuteImpl(Block &block, size_t result_idx,
                             size_t input_rows_count) const = 0;

  // before execute function we should construct new column push it to block
  virtual std::shared_ptr<ValueType> GetResultType() const = 0;

  virtual Status
  ResolveResultType(Block &block,
                    std::shared_ptr<ValueType> &result_type) const {
    result_type = GetResultType();
    return Status::OK();
  }
};
} // namespace DB
