#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <filesystem>
#include <fstream>
#include <ios>

namespace DB {
// Write Ahead Log
class WAL {
  bool write_log_;
  bool defer_flush_{false};
  std::filesystem::path path_;
  std::fstream fs_;

public:
  WAL() = default;
  // path 应该是完整的 WAL 文件路径
  WAL(std::filesystem::path path, bool write_log, bool rewrite = false)
      : path_(std::move(path)), write_log_(write_log) {
    if (write_log) {
      if (rewrite) {
        fs_.open(path_, std::ios_base::out);
        fs_.close();
      } else {
        fs_.open(path_, std::ios::binary | std::ios_base::in |
                            std::ios_base::out | std::ios::app);
      }
    }
  }

  ~WAL() { fs_.close(); }

  void StartWriteLog() { write_log_ = true; }

  void StopWriteLog() { write_log_ = false; }

  void SetDeferFlush(bool defer) { defer_flush_ = defer; }

  void Flush();

  void Finish() { fs_.close(); }

  std::filesystem::path GetPath() const { return path_; }

  static void Remove(const std::filesystem::path &path) {
    std::filesystem::remove(path);
  }

  Status WriteSlice(const Slice &key, const Slice &value);

  bool ReadFromLogFile(Slice *key, Slice *value);
};
} // namespace DB