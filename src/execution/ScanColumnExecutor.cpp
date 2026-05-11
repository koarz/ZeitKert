#include "execution/ScanColumnExecutor.hpp"
#include "common/Status.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnWithNameType.hpp"

#include <memory>
#include <tuple>

namespace DB {

Status ScanColumnExecutor::Init() {
  if (!lsm_tree_) {
    return Status::Error(ErrorCode::NotFound, "Table storage not initialized");
  }

  // 优先复用 Filter 在 Init 阶段填入缓存的过滤后数据
  if (FilteredDataCache::IsActive()) {
    column_ = FilteredDataCache::Get(column_meta_->name_);
  }

  // 缓存未命中则真正发起列扫描
  if (!column_) {
    std::ignore = lsm_tree_->ScanColumn(column_idx_, column_);
  }
  emitted_ = false;
  return Status::OK();
}

Status ScanColumnExecutor::Next(DataChunk &out) {
  // 第二次及以后调用：返回空 chunk 表示 EOF
  if (emitted_ || !column_) {
    return Status::OK();
  }

  // 第一次调用：把整列封装成命名列发射出去
  // 注：这里保持单批次发射以保留 ColumnVector::DataSpan 的 zero-copy 语义
  auto named = std::make_shared<ColumnWithNameType>(
      column_, column_meta_->name_, column_meta_->type_);
  out.AddColumn(named, column_->Size());
  emitted_ = true;
  return Status::OK();
}

} // namespace DB
