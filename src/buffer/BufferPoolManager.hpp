#pragma once

#include "buffer/Replacer.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/Page.hpp"
#include "storage/disk/DiskManager.hpp"

#include <condition_variable>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <unordered_map>

namespace DB {
class BufferPoolManager {
  struct PageKey {
    std::filesystem::path path;
    page_id_t page_id;

    bool operator==(const PageKey &other) const noexcept {
      return page_id == other.page_id && path == other.path;
    }
  };

  struct PageKeyHash {
    size_t operator()(const PageKey &key) const noexcept {
      size_t seed = std::hash<std::filesystem::path>{}(key.path);
      // https://zhuanlan.zhihu.com/p/574573421
      seed ^= std::hash<page_id_t>{}(key.page_id) + 0x9e3779b9 + (seed << 6) +
              (seed >> 2);
      return seed;
    }
  };

  struct InFlight {
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    Status status = Status::OK();
  };

  std::mutex latch_;
  std::mutex path_latch_; // 保护 path_map_ 的文件流打开与查找。
  Page *pages_;
  std::mutex *page_locks_;
  const size_t pool_size_;
  std::unique_ptr<Replacer> replacer_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::map<std::filesystem::path, std::fstream> path_map_;
  std::unordered_map<PageKey, frame_id_t, PageKeyHash> page_table_;
  // 记录正在加载的页，避免同页被重复读。
  std::unordered_map<PageKey, std::shared_ptr<InFlight>, PageKeyHash> inflight_;

  std::fstream &GetFileStream(const std::filesystem::path &path);

public:
  BufferPoolManager(size_t pool_size,
                    std::shared_ptr<DiskManager> disk_manager);
  ~BufferPoolManager();

  Status FetchPage(std::filesystem::path column_path, page_id_t page_id,
                   Page *&page);

  Status UnPinPage(std::filesystem::path column_path, page_id_t page_id);

  Status FlushPage(frame_id_t frame_id);

  Status FlushAllPage();
};
} // namespace DB
