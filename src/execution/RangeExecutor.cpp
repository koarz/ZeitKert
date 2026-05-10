#include "execution/RangeExecutor.hpp"
#include "common/Status.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "type/Int.hpp"

#include <memory>

namespace DB {

Status RangeExecutor::Init() {
  emitted_ = false;
  return Status::OK();
}

Status RangeExecutor::Next(DataChunk &out) {
  // 一次性发射全部序列；后续调用直接返回空 chunk 表示 EOF
  if (emitted_) {
    return Status::OK();
  }

  auto column = std::make_shared<ColumnVector<int>>();

  // 预估序列长度，避免反复 reallocate
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

  auto named = std::make_shared<ColumnWithNameType>(column, "range",
                                                    std::make_shared<Int>());
  out.AddColumn(named, column->Size());
  emitted_ = true;
  return Status::OK();
}

} // namespace DB
