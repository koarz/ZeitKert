#pragma once

#include "buffer/Replacer.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/Page.hpp"
#include "storage/disk/DiskManager.hpp"

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>

namespace DB {
class BufferPoolManager {
  std::mutex latch_;
  Page *pages_;
  const size_t pool_size_;
  std::unique_ptr<Replacer> replacer_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::map<std::filesystem::path, std::fstream> path_map_;

public:
  BufferPoolManager(size_t pool_size,
                    std::shared_ptr<DiskManager> disk_manager);
  ~BufferPoolManager();

  Status FetchPage(std::filesystem::path column_path, page_id_t page_id,
                   Page *&page);

  Status UnPinPage(std::filesystem::path column_path, page_id_t page_id);

  Status FlushPage(std::filesystem::path column_path, page_id_t page_id);

  Status FlushAllPage();
};
} // namespace DB