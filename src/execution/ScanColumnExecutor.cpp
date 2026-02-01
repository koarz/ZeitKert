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

  // 如果 Filter 已经预先扫描并过滤了数据，直接使用过滤后的列
  if (FilteredDataCache::IsActive()) {
    column = FilteredDataCache::Get(column_meta_->name_);
  }

  // 如果缓存中没有，正常扫描
  if (!column) {
    std::ignore = lsm_tree_->ScanColumn(column_idx_, column);
  }

  schema_->GetColumns().emplace_back(std::make_shared<ColumnWithNameType>(
      column, column_meta_->name_, column_meta_->type_));
  return Status::OK();
}
} // namespace DB
