#include "storage/disk/DiskManager.hpp"
#include "common/Status.hpp"
#include <filesystem>

namespace DB {
Status DiskManager::CreateDatabase(std::string name) {
  auto path = path_ / name;
  try {
    if (!std::filesystem::create_directory(path)) {
      return Status::Error(ErrorCode::CreateError,
                           "The Database Already Exists");
    }
  } catch (const std::filesystem::filesystem_error &e) {
    return Status::Error(ErrorCode::CreateError,
                         std::format("Error creating directory: {}", e.what()));
  }
  return Status::OK();
}
} // namespace DB