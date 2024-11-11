#include "buffer/BufferPoolManager.hpp"
#include "buffer/replacer/LRUReplacer.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/Page.hpp"
#include <algorithm>
#include <memory>
#include <mutex>
#include <tuple>

namespace DB {
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     std::shared_ptr<DiskManager> disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  replacer_ = std::make_unique<LRUReplacer>(pool_size);
  pages_ = new Page[pool_size];
}

BufferPoolManager::~BufferPoolManager() {
  std::ignore = FlushAllPage();
  delete[] pages_;
  for (auto &fs : path_map_) {
    fs.second.close();
  }
}

Status BufferPoolManager::FetchPage(std::filesystem::path column_path,
                                    page_id_t page_id, Page *&page) {
  std::unique_lock<std::mutex> lock(latch_);
  auto OnPage = [&](Page *page) {
    return page->GetPath() == column_path && page->GetPageId() == page_id;
  };
  for (int i = 0; i < pool_size_; i++) {
    if (OnPage(pages_ + i)) {
      replacer_->Access(i);
      return Status::OK();
    }
  }
  frame_id_t evict;
  replacer_->Evict(&evict);
  if (evict == -1) {
    return Status::Error(ErrorCode::BufferPoolError,
                         "Buffer pool was full, not page evictable");
  }
  replacer_->Access(evict);
  lock.unlock();
  page = pages_ + evict;
  auto page_lock = page->GetWriteLock();
  Status status = Status::OK();
  if (page->IsDirty()) {
    auto &fs = path_map_[page->GetPath()];
    if (!fs.is_open()) {
      fs.open(page->GetPath(),
              std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
    }
    status = disk_manager_->WritePage(fs, page->GetPageId(), page->GetData());
    if (!status.ok()) {
      return status;
    }
  }
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->path_ = column_path;
  status =
      disk_manager_->ReadPage(path_map_[column_path], page_id, page->data_);
  return status;
}

Status BufferPoolManager::UnPinPage(std::filesystem::path column_path,
                                    page_id_t page_id) {
  std::unique_lock<std::mutex> lock(latch_);
  auto OnPage = [&](Page *page) {
    return page->GetPath() == column_path && page->GetPageId() == page_id;
  };
  for (int i = 0; i < pool_size_; i++) {
    if (OnPage(pages_ + i)) {
      replacer_->UnPin(i);
      if (replacer_->GetPinCount(i) == 0) {
        if (Status status = FlushPage(column_path, page_id); !status.ok()) {
          return status;
        }
      }
      break;
    }
  }
  return Status::OK();
}

Status BufferPoolManager::FlushPage(std::filesystem::path column_path,
                                    page_id_t page_id) {
  auto OnPage = [&](Page *page) {
    return page->GetPath() == column_path && page->GetPageId() == page_id;
  };
  for (int i = 0; i < pool_size_; i++) {
    if (OnPage(pages_ + i)) {
      auto &page = pages_[i];
      auto page_lock = page.GetWriteLock();
      Status status;
      auto &fs = path_map_[column_path];
      if (!fs.is_open()) {
        fs.open(page.GetPath(), std::ios::binary | std::ios::in |
                                    std::ios::app | std::ios::out);
      }
      status = disk_manager_->WritePage(fs, page_id, page.GetData());
      if (!status.ok()) {
        return status;
      }
      page.is_dirty_ = false;
    }
  }
  return Status::OK();
}

Status BufferPoolManager::FlushAllPage() {
  for (int i = 0; i < pool_size_; i++) {
    std::ignore = FlushPage(pages_[i].GetPath(), pages_[i].GetPageId());
  }
  return Status::OK();
}
} // namespace DB