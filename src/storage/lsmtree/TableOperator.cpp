#include "storage/lsmtree/TableOperator.hpp"
#include "common/Config.hpp"
#include "common/Logger.hpp"
#include "common/Status.hpp"
#include "fmt/format.h"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/builder/SSTableBuilder.hpp"
#include "storage/lsmtree/iterator/Iterator.hpp"
#include "storage/lsmtree/iterator/MergeIterator.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <tuple>

namespace DB {
Status TableOperator::BuildSSTable(
    std::filesystem::path path, uint32_t &table_id,
    std::vector<MemTableRef> &memtables,
    const std::vector<std::shared_ptr<ValueType>> &column_types,
    uint16_t primary_key_idx, SSTableRef &sstable_meta) {
  SSTableBuilder builder(path, table_id, column_types, primary_key_idx);
  std::vector<std::shared_ptr<Iterator>> iters;
  // 新到旧合并 memtable
  for (auto it = memtables.rbegin(); it != memtables.rend(); it++) {
    iters.push_back(
        std::make_shared<MemTableIterator>((*it)->MakeNewIterator()));
  }
  // 获取主键类型用于正确比较
  auto pk_type = column_types[primary_key_idx]->GetType();
  MergeIterator iter(std::move(iters), pk_type);
  while (iter.Valid()) {
    // 跳过 tombstone（删除标记，value 为空）
    if (iter.GetValue().Size() == 0) {
      iter.Next();
      continue;
    }
    if (builder.Add(iter.GetKey(), iter.GetValue()) == false) {
      break;
    }
    iter.Next();
  }
  // 溢出数据不再写入 WAL，单个 MemTable 大小已约等于 SSTABLE_SIZE
  // 如果有溢出数据，直接丢弃（正常情况下不应该有溢出）
  table_id++;
  auto s = builder.Finish();
  sstable_meta = builder.BuildSSTableMeta();
  LOG_INFO("BuildSSTable: table_id={}, rowgroups={}", sstable_meta->sstable_id_,
           sstable_meta->rowgroups_.size());
  return s;
}

static constexpr uint32_t kSSTableMagic = 0x5A4B5254; // ZKRT
static constexpr uint16_t kSSTableVersion = 2;

static Status ReadRange(std::filesystem::path path, uint32_t offset,
                        uint32_t size,
                        std::shared_ptr<BufferPoolManager> buffer_pool,
                        std::string &out) {
  out.resize(size);
  if (size == 0) {
    return Status::OK();
  }
  // 通过 bufferpool 按页读取文件片段
  uint32_t end = offset + size;
  uint32_t start_page = offset / DEFAULT_PAGE_SIZE;
  uint32_t end_page = (end - 1) / DEFAULT_PAGE_SIZE;
  for (uint32_t page_id = start_page; page_id <= end_page; page_id++) {
    Page *page = nullptr;
    auto status = buffer_pool->FetchPage(path, page_id, page);
    if (!status.ok()) {
      return status;
    }
    auto lock = page->GetReadLock();
    uint32_t page_start = page_id * DEFAULT_PAGE_SIZE;
    uint32_t copy_start = std::max<uint32_t>(offset, page_start);
    uint32_t copy_end = std::min<uint32_t>(
        end, page_start + static_cast<uint32_t>(DEFAULT_PAGE_SIZE));
    std::memcpy(out.data() + (copy_start - offset),
                page->GetData() + (copy_start - page_start),
                copy_end - copy_start);
    std::ignore = buffer_pool->UnPinPage(path, page_id);
  }
  return Status::OK();
}

Status TableOperator::ReadSSTable(
    std::filesystem::path path, SSTableRef sstable_meta,
    const std::vector<std::shared_ptr<ValueType>> &column_types,
    std::shared_ptr<BufferPoolManager> buffer_pool) {
  path = path / fmt::format("{}.sst", sstable_meta->sstable_id_);
  if (!std::filesystem::exists(path)) {
    return Status::Error(
        ErrorCode::FileNotOpen,
        fmt::format("The sstable {} was not exist", path.filename().c_str()));
  }
  auto file_size = std::filesystem::file_size(path);
  constexpr uint32_t footer_size = sizeof(uint32_t) * 4 + sizeof(uint16_t) * 4;
  if (file_size < footer_size) {
    return Status::Error(ErrorCode::IOError, "SSTable footer truncated");
  }

  std::string footer_blob;
  // Footer 固定长度从文件末尾读取
  auto status = ReadRange(path, static_cast<uint32_t>(file_size - footer_size),
                          footer_size, buffer_pool, footer_blob);
  if (!status.ok()) {
    return status;
  }
  const Byte *p = reinterpret_cast<const Byte *>(footer_blob.data());
  const Byte *end = p + footer_blob.size();
  auto read = [&](auto &v) {
    if (p + sizeof(v) > end) {
      return false;
    }
    std::memcpy(&v, p, sizeof(v));
    p += sizeof(v);
    return true;
  };

  uint32_t meta_offset = 0;
  uint32_t meta_size = 0;
  uint32_t rowgroup_count = 0;
  uint16_t column_count = 0;
  uint16_t primary_key_idx = 0;
  uint16_t version = 0;
  uint16_t reserved = 0;
  uint32_t magic = 0;
  if (!read(meta_offset) || !read(meta_size) || !read(rowgroup_count) ||
      !read(column_count) || !read(primary_key_idx) || !read(version) ||
      !read(reserved) || !read(magic)) {
    return Status::Error(ErrorCode::IOError, "SSTable footer corrupted");
  }
  // 校验版本和列数
  if (magic != kSSTableMagic || version != kSSTableVersion) {
    return Status::Error(ErrorCode::IOError, "SSTable version mismatch");
  }
  if (column_count != column_types.size()) {
    return Status::Error(ErrorCode::IOError, "SSTable column count mismatch");
  }
  if (meta_offset + meta_size > file_size) {
    return Status::Error(ErrorCode::IOError, "SSTable meta out of range");
  }

  std::string meta_blob;
  // 读取 RowGroup 元数据段
  status = ReadRange(path, meta_offset, meta_size, buffer_pool, meta_blob);
  if (!status.ok()) {
    return status;
  }
  p = reinterpret_cast<const Byte *>(meta_blob.data());
  end = p + meta_blob.size();
  sstable_meta->rowgroups_.clear();
  sstable_meta->rowgroups_.reserve(rowgroup_count);
  for (uint32_t i = 0; i < rowgroup_count; i++) {
    RowGroupMeta meta;
    if (!RowGroupMeta::Deserialize(p, end, column_types, meta)) {
      return Status::Error(ErrorCode::IOError, "SSTable meta corrupted");
    }
    sstable_meta->rowgroups_.push_back(std::move(meta));
  }
  // 数据段使用 mmap 读取
  sstable_meta->rowgroup_count_ = rowgroup_count;
  sstable_meta->column_count_ = column_count;
  sstable_meta->primary_key_idx_ = primary_key_idx;
  sstable_meta->data_file_ = std::make_shared<MMapFile>(path);
  LOG_INFO("ReadSSTable: id={}, rowgroups={}, columns={}, pk_idx={}",
           sstable_meta->sstable_id_, rowgroup_count, column_count,
           primary_key_idx);
  return Status::OK();
}
} // namespace DB
