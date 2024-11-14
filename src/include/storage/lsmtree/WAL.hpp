#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"
#include <filesystem>
#include <fstream>
#include <ios>

namespace DB {
// Write Ahead Log
class WAL {
  // the path should named column.wal
  std::filesystem::path path_;
  // the fs for write file
  std::fstream fs_;

public:
  WAL() = default;
  WAL(std::filesystem::path path) : path_(path) {
    fs_.open(path_,
             std::ios_base::in | std::ios_base::out | std::ios_base::app);
  }

  ~WAL() { fs_.close(); }

  Status WriteSlice(Slice key, Slice value);

  bool ReadFromLogFile(Slice *key, Slice *value);
};
} // namespace DB