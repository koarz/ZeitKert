#pragma once

#include "common/Config.hpp"

#include <filesystem>

namespace DB {
class MMapFile {
  int fd_{-1};
  size_t size_{0};
  Byte *data_{nullptr};

public:
  explicit MMapFile(const std::filesystem::path &path);
  ~MMapFile();

  // 使用 mmap 映射只读文件
  bool Valid() const { return data_ != nullptr; }
  const Byte *Data() const { return data_; }
  size_t Size() const { return size_; }
};
} // namespace DB
