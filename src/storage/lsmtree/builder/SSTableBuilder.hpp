#pragma once

#include "common/Config.hpp"
#include "common/Status.hpp"
#include "fmt/format.h"
#include "storage/lsmtree/Coding.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/builder/BlockBuilder.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>

namespace DB {
class SSTableBuilder {
  uint32_t table_id_;
  std::filesystem::path path_;

  std::vector<Slice> index_;
  std::vector<uint32_t> offsets_;
  std::vector<BlockBuilder> blocks_;

public:
  SSTableBuilder(std::filesystem::path path, uint32_t table_num)
      : table_id_(table_num),
        path_(std::move(path / fmt::format("{}.sst", table_num))) {
    std::filesystem::create_directory(path);
    blocks_.emplace_back(BlockBuilder{});
  }

  [[nodiscard]] bool Add(Slice &key, Slice &value) {
    if (blocks_.size() > SSTABLE_SIZE / DEFAULT_PAGE_SIZE) {
      return false;
    }
    auto &block = *blocks_.rbegin();
    auto t = block.Add(key, value);
    if (t == -1) {
      blocks_.emplace_back(BlockBuilder{});
      auto &block = *blocks_.rbegin();
      t = block.Add(key, value);
    }
    offsets_.push_back(t + (blocks_.size() - 1) * DEFAULT_PAGE_SIZE);
    return true;
  }

  Status Finish() {
    if (blocks_.rbegin()->IsEmpty()) {
      blocks_.pop_back();
    }
    // start write file
    std::ofstream fs;
    fs.open(path_, std::ios::trunc | std::ios::binary | std::ios::out);
    int i = 0;
    for (auto &block : blocks_) {
      auto &data = block.GetData();
      fs.seekp(i * DEFAULT_PAGE_SIZE);
      fs.write(data.data(), data.size());
      index_.push_back(block.GetLastKey());
      i++;
    }

    // write max key
    for (auto &key : index_) {
      uint32_t size = key.Size() + 4;
      auto buffer = std::make_unique<Byte[]>(size);
      auto *p = buffer.get();
      p = EncodeUInt32(p, key.Size());
      std::memcpy(p, key.GetData(), key.Size());
      fs.write(buffer.get(), size);
    }

    // write every key's offset
    // do we need offsets?
    // yes we need, offset mainly for the SSTableIterator
    {
      auto buffer = std::make_unique<Byte[]>(offsets_.size() * 4);
      auto *p = buffer.get();
      for (auto &v : offsets_) {
        p = EncodeUInt32(p, v);
      }
      fs.write(buffer.get(), offsets_.size() * 4);
    }

    // write how many blocks and offsets
    uint32_t offset_num = offsets_.size();
    fs.write(reinterpret_cast<char *>(&offset_num), 4);
    uint16_t block_num = blocks_.size();
    fs.write(reinterpret_cast<char *>(&block_num), 2);
    fs.close();
    return Status::OK();
  };

  SSTableRef BuildSSTableMeta() {
    return std::make_shared<SSTable>(table_id_, blocks_.size(), index_,
                                     offsets_);
  }
};
} // namespace DB