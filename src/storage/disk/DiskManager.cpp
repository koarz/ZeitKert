#include "storage/disk/DiskManager.hpp"
#include "common/Status.hpp"
#include <filesystem>
#include <mutex>

namespace DB {
Status DiskManager::CreateDatabase(std::string name) {
  std::unique_lock<std::mutex> lock(latch_);
  auto path = path_ / name;
  try {
    if (!std::filesystem::create_directory(path)) {
      return Status::Error(ErrorCode::CreateError,
                           "The Database Already Exists");
    }
  } catch (const std::filesystem::filesystem_error &e) {
    return Status::Error(ErrorCode::CreateError,
                         std::format("Error creating database: {}", e.what()));
  }
  return Status::OK();
}

Status DiskManager::OpenDatabase(std::string name) {
  std::unique_lock<std::mutex> lock(latch_);
  auto path = path_ / name;
  if (!path.has_filename()) {
    return Status::Error(ErrorCode::DatabaseNotExists,
                         "The database now exists");
  }
  path_ = path;
  return Status::OK();
}
} // namespace DB