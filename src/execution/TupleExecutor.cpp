#include "execution/TupleExecutor.hpp"
#include "common/Status.hpp"

namespace DB {

Status TupleExecutor::Init() {
  emitted_ = false;
  for (auto &child : children_) {
    auto status = child->Init();
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status TupleExecutor::Next(DataChunk &out) {
  // 一次性发射：从每个子节点拉一个 chunk 拼成横向元组
  if (emitted_) {
    return Status::OK();
  }

  size_t row_count = 0;
  for (auto &child : children_) {
    DataChunk child_chunk;
    auto status = child->Next(child_chunk);
    if (!status.ok()) {
      return status;
    }
    if (child_chunk.IsEmpty()) {
      // 任一子节点没有数据则整个 tuple 视为 EOF
      out.Reset();
      emitted_ = true;
      return Status::OK();
    }
    row_count = std::max(row_count, child_chunk.row_count());
    for (auto &col : child_chunk.columns()) {
      out.AddColumn(col, col->Size());
    }
  }
  // 显式覆盖行数（DataChunk::AddColumn 默认按第一列长度推断）
  out.set_row_count(row_count);
  emitted_ = true;
  return Status::OK();
}

} // namespace DB
