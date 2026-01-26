#include "buffer/BufferPoolManager.hpp"
#include "buffer/replacer/ClockReplacer.hpp"
#include "buffer/replacer/LRUReplacer.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/Page.hpp"

#include <memory>
#include <mutex>
#include <tuple>
#include <utility>

namespace DB {
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     std::shared_ptr<DiskManager> disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  replacer_ = std::make_unique<ClockReplacer>(pool_size);
  pages_ = new Page[pool_size];
  page_locks_ = new std::mutex[pool_size];
  page_table_.reserve(pool_size);
}

BufferPoolManager::~BufferPoolManager() {
  std::ignore = FlushAllPage();
  delete[] pages_;
  delete[] page_locks_;
  for (auto &fs : path_map_) {
    fs.second.close();
  }
}

std::fstream &
BufferPoolManager::GetFileStream(const std::filesystem::path &path) {
  std::unique_lock<std::mutex> lock(path_latch_);
  auto &fs = path_map_[path];
  if (!fs.is_open()) {
    fs.open(path,
            std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
    fs.close();
    fs.open(path, std::ios::binary | std::ios::in | std::ios::out);
  }
  return fs;
}

Status BufferPoolManager::FetchPage(std::filesystem::path column_path,
                                    page_id_t page_id, Page *&page) {
  page = nullptr;
  PageKey key{column_path, page_id};
  std::shared_ptr<InFlight> inflight;

  // 通过 in-flight 机制避免同一页被重复加载
  // 只有第一个发现缺页的线程负责加载，其余线程等待加载结果
  while (true) {
    bool is_loader = false;
    {
      std::unique_lock<std::mutex> lock(latch_);
      auto it = page_table_.find(key);
      if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        page = pages_ + frame_id;
        replacer_->Access(frame_id);
        return Status::OK();
      }
      auto inflight_it = inflight_.find(key);
      if (inflight_it != inflight_.end()) {
        inflight = inflight_it->second;
      } else {
        inflight = std::make_shared<InFlight>();
        inflight_.emplace(key, inflight);
        is_loader = true;
      }
    }

    if (is_loader) {
      break;
    }

    // 等待加载线程完成，并复用它的结果
    std::unique_lock inflight_lock(inflight->mutex);
    inflight->cv.wait(inflight_lock, [&] { return inflight->done; });
    Status status = inflight->status;
    inflight_lock.unlock();
    if (!status.ok()) {
      return status;
    }
  }

  auto finish_inflight = [&](const Status &status) {
    {
      std::unique_lock inflight_lock(inflight->mutex);
      inflight->status = status;
      inflight->done = true;
    }
    inflight->cv.notify_all();
  };

  Status status = Status::OK();
  frame_id_t evict = -1;
  Page *target = nullptr;
  std::filesystem::path old_path;
  page_id_t old_page_id = INVALID_PAGE_ID;
  bool old_dirty = false;
  PageKey evict_key{};
  std::shared_ptr<InFlight> evict_inflight;
  bool block_old = false;

  // 在全局锁内选择牺牲页并更新页表
  // 这里持有 latch_ 只处理元数据，不进行磁盘 IO
  std::unique_lock<std::mutex> lock(latch_);
  replacer_->Evict(&evict);
  if (evict == -1) {
    inflight_.erase(key);
    lock.unlock();
    status = Status::Error(ErrorCode::BufferPoolError,
                           "Buffer pool was full, not page evictable");
    finish_inflight(status);
    return status;
  }

  replacer_->Access(evict);
  target = pages_ + evict;
  std::unique_lock page_lock(page_locks_[evict]);
  old_path = target->path_;
  old_page_id = target->page_id_;
  old_dirty = target->IsDirty();
  if (old_page_id != INVALID_PAGE_ID) {
    page_table_.erase(PageKey{old_path, old_page_id});
  }
  if (old_page_id != INVALID_PAGE_ID && old_dirty) {
    evict_key = PageKey{old_path, old_page_id};
    auto evict_it = inflight_.find(evict_key);
    if (evict_it != inflight_.end()) {
      evict_inflight = evict_it->second;
    } else {
      evict_inflight = std::make_shared<InFlight>();
      inflight_.emplace(evict_key, evict_inflight);
    }
    block_old = true;
  }
  lock.unlock();

  // 脏页写回放在锁外，但阻塞旧页访问直至写完
  // 这样既避免长时间持锁，又保证被逐出的旧页不会被并发访问
  auto finish_evict = [&] {
    if (!block_old) {
      return;
    }
    {
      std::unique_lock inflight_lock(evict_inflight->mutex);
      evict_inflight->status = Status::OK();
      evict_inflight->done = true;
    }
    evict_inflight->cv.notify_all();
  };

  if (old_page_id != INVALID_PAGE_ID && old_dirty) {
    auto &old_fs = GetFileStream(old_path);
    status = disk_manager_->WritePage(old_fs, old_page_id, target->GetData());
  }
  // 写回失败时需要恢复元数据，并唤醒等待者。
  if (!status.ok()) {
    page_lock.unlock();
    lock.lock();
    if (old_page_id != INVALID_PAGE_ID) {
      page_table_.emplace(PageKey{old_path, old_page_id}, evict);
    }
    replacer_->UnPin(evict);
    if (block_old) {
      inflight_.erase(evict_key);
    }
    inflight_.erase(key);
    lock.unlock();
    page = nullptr;
    finish_evict();
    finish_inflight(status);
    return status;
  }

  target->page_id_ = page_id;
  target->path_ = column_path;
  target->is_dirty_ = false;

  // 读取新页内容，失败时回滚目标帧状态。
  auto &fs = GetFileStream(target->path_);
  status = disk_manager_->ReadPage(fs, page_id, target->data_);
  if (!status.ok()) {
    target->page_id_ = INVALID_PAGE_ID;
    target->path_.clear();
    target->is_dirty_ = false;
    page_lock.unlock();
    lock.lock();
    replacer_->UnPin(evict);
    if (block_old) {
      inflight_.erase(evict_key);
    }
    inflight_.erase(key);
    lock.unlock();
    page = nullptr;
    finish_evict();
    finish_inflight(status);
    return status;
  }

  page_lock.unlock();
  lock.lock();
  // 读成功后写回页表并唤醒等待者。
  // 这里重新持有 latch_ 只更新元数据。
  page_table_.emplace(PageKey{column_path, page_id}, evict);
  if (block_old) {
    inflight_.erase(evict_key);
  }
  inflight_.erase(key);
  lock.unlock();

  page = target;
  finish_evict();
  finish_inflight(status);
  return status;
}

Status BufferPoolManager::UnPinPage(std::filesystem::path column_path,
                                    page_id_t page_id) {
  std::unique_lock<std::mutex> lock(latch_);
  auto it = page_table_.find(PageKey{column_path, page_id});
  if (it != page_table_.end()) {
    replacer_->UnPin(it->second);
  }
  return Status::OK();
}

Status BufferPoolManager::FlushPage(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= pool_size_) {
    return Status::Error(ErrorCode::BufferPoolError,
                         "Invalid frame id for flush");
  }
  std::unique_lock<std::mutex> lock(latch_);
  std::unique_lock page_lock(page_locks_[frame_id]);
  auto &page = pages_[frame_id];
  if (page.page_id_ == INVALID_PAGE_ID || !page.IsDirty()) {
    return Status::OK();
  }
  auto &fs = GetFileStream(page.path_);
  Status status =
      disk_manager_->WritePage(fs, page.GetPageId(), page.GetData());
  if (!status.ok()) {
    return status;
  }
  page.is_dirty_ = false;
  return Status::OK();
}

Status BufferPoolManager::FlushAllPage() {
  for (int i = 0; i < pool_size_; i++) {
    std::ignore = FlushPage(i);
  }
  return Status::OK();
}
} // namespace DB
