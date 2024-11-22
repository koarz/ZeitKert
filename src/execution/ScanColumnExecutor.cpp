#include "execution/ScanColumnExecutor.hpp"
#include "common/Status.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnWithNameType.hpp"

#include <memory>

namespace DB {
Status ScanColumnExecutor::Execute() {
  ColumnPtr column;
  std::ignore = column_meta_->lsm_tree_->ScanColumn(column);
  schema_->GetColumns().emplace_back(std::make_shared<ColumnWithNameType>(
      column, column_meta_->name_, column_meta_->type_));
  return Status::OK();
}
} // namespace DB