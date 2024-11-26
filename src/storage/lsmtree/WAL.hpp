#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <filesystem>
#include <fstream>
#include <ios>

namespace DB {
// Write Ahead Log
class WAL {
  // TODO:
  // when write immutable table start, we need a new file to become wal
  // if system crashed we need read old file first

  // WARNING:
  // lsm tree write log signal can't trans to their
  bool write_log_;
  // the path should named column.wal
  std::filesystem::path path_;
  // the fs for write file
  std::fstream fs_;

public:
  WAL() = default;
  WAL(std::filesystem::path path, bool write_log, bool rewrite = false)
      : path_(path.concat(".wal")), write_log_(write_log) {
    if (write_log) {
      if (rewrite) {
        fs_.open(path_, std::ios::trunc);
      } else {
        // first start only read when read eof the fs will close(maybe)
        fs_.open(path_, std::ios::binary | std::ios_base::in);
      }
    }
  }

  ~WAL() { fs_.close(); }

  void StartWriteLog() { write_log_ = true; }

  void StopWriteLog() { write_log_ = false; }

  void Finish() { fs_.close(); }

  Status WriteSlice(const Slice &key, const Slice &value);

  bool ReadFromLogFile(Slice *key, Slice *value);
};
} // namespace DB