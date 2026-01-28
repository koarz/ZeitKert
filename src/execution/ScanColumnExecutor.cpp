#include "execution/ScanColumnExecutor.hpp"
#include "common/Status.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnWithNameType.hpp"

#include <memory>
#include <tuple>

namespace DB {
Status ScanColumnExecutor::Execute() {
  if (!lsm_tree_) {
    return Status::Error(ErrorCode::NotFound, "Table storage not initialized");
  }
  ColumnPtr column;
  std::ignore = lsm_tree_->ScanColumn(column_idx_, column);
  schema_->GetColumns().emplace_back(std::make_shared<ColumnWithNameType>(
      column, column_meta_->name_, column_meta_->type_));
  return Status::OK();
}
} // namespace DB
