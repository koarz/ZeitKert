#include "storage/lsmtree/TableOperator.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "fmt/format.h"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/builder/SSTableBuilder.hpp"
#include "storage/lsmtree/iterator/Iterator.hpp"
#include "storage/lsmtree/iterator/MergeIterator.hpp"

#include <filesystem>
#include <memory>

namespace DB {
Status TableOperator::BuildSSTable(std::filesystem::path path,
                                   uint32_t &table_id,
                                   std::vector<MemTableRef> &memtables) {
  SSTableBuilder builder(path, table_id);
  std::vector<std::shared_ptr<Iterator>> iters;
  for (auto it = memtables.rbegin(); it != memtables.rend(); it++) {
    iters.push_back(
        std::make_shared<MemTableIterator>((*it)->MakeNewIterator()));
  }
  MergeIterator iter(std::move(iters));
  while (iter.Valid()) {
    if (builder.Add(iter.GetKey(), iter.GetValue()) == false) {
      auto s = builder.Finish();
      if (!s.ok()) {
        return s;
      }
      // write other data to wal file
      // use rewrite flag for open a new file
      WAL wal(path, true, true);
      while (iter.Valid()) {
        s = wal.WriteSlice(iter.GetKey(), iter.GetValue());
        if (!s.ok()) {
          return s;
        }
        iter.Next();
      }
    }
    iter.Next();
  }
  auto s = builder.Finish();
  return s;
}

Status TableOperator::ReadSSTable(std::filesystem::path path,
                                  SSTableRef sstable_meta) {
  path = path / fmt::format("{}.sst", sstable_meta->sstable_id_);
  auto file_size = std::filesystem::file_size(path);
  std::ifstream fs;
  fs.open(path, std::ios::binary | std::ios::in);
  if (!fs.is_open()) {
    return Status::Error(
        ErrorCode::FileNotOpen,
        fmt::format("The sstable {} was not exist", path.filename().c_str()));
  }
  // read offset and blocks num
  fs.seekg(file_size - 6);
  uint32_t offset_num;
  fs.read(reinterpret_cast<char *>(&offset_num), 4);
  fs.read(reinterpret_cast<char *>(&sstable_meta->num_of_blocks_), 2);
  fs.seekg(file_size - 6 - 4 * offset_num);
  for (int i = 0; i < offset_num; i++) {
    uint32_t offset;
    fs.read(reinterpret_cast<char *>(&offset), 4);
  }
  fs.seekg(sstable_meta->num_of_blocks_ * DEFAULT_PAGE_SIZE);
  // read index
  for (int i = 0; i < sstable_meta->num_of_blocks_; i++) {
    uint32_t klen;
    fs.read(reinterpret_cast<char *>(&klen), 4);
    auto buffer = std::make_unique<Byte[]>(klen);
    fs.read(buffer.get(), klen);
    sstable_meta->index_.push_back(
        Slice{buffer.get(), static_cast<uint16_t>(klen)});
  }
  return Status::OK();
}
} // namespace DB