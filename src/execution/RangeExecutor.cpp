#include "execution/RangeExecutor.hpp"
#include "common/Status.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/Int.hpp"

#include <memory>

namespace DB {
Status RangeExecutor::Execute() {
  auto column = std::make_shared<ColumnVector<int>>();

  int64_t count = 0;
  if (step_ > 0 && stop_ > start_) {
    count = (stop_ - start_ + step_ - 1) / step_;
  } else if (step_ < 0 && stop_ < start_) {
    count = (start_ - stop_ - step_ - 1) / (-step_);
  }

  column->Reserve(static_cast<size_t>(count));
  for (int64_t v = start_; step_ > 0 ? v < stop_ : v > stop_; v += step_) {
    column->Insert(static_cast<int>(v));
  }

  schema_->GetColumns().emplace_back(std::make_shared<ColumnWithNameType>(
      column, "range", std::make_shared<Int>()));
  return Status::OK();
}
} // namespace DB
