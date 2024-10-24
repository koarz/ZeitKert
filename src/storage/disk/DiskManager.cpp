#include "storage/disk/DiskManager.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include <filesystem>
#include <iostream>

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
Status DiskManager::DropDatabase(std::string &name) {
  auto path = path_ / name;
  try {
    if (!std::filesystem::exists(path)) {
      return Status::Error(ErrorCode::DropError, "The Database Is not Exist");
    }
    if (!std::filesystem::remove(path)) {
      return Status::Error(ErrorCode::DropError,
                           "The Database can't be dropped");
    }
  } catch (const std::filesystem::filesystem_error &e) {
    return Status::Error(ErrorCode::DropError,
                         std::format("Error dropping database: {}", e.what()));
  }
  return Status::OK();
}
Status DiskManager::ShowDatabase() {
  auto path = path_;
  try {
    if (!std::filesystem::exists(path)) {
      return Status::Error(ErrorCode::CreateError, "The Database Is not Exist");
    }
    std::vector<ResultSet::Row> result_row;
    for (const auto &entry : std::filesystem::directory_iterator(path)) {
      result_row.emplace_back(ResultSet::Row(entry.path().filename()));
    }
    ResultSet::PrintResult(result_row, "Database");
  } catch (const std::filesystem::filesystem_error &e) {
    return Status::Error(ErrorCode::CreateError,
                         std::format("Error creating directory: {}", e.what()));
  }
  return Status::OK();
}
} // namespace DB