#include "storage/disk/DiskManager.hpp"
#include "common/ResultSet.hpp"
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

Status DiskManager::OpenDatabase(std::string name) {
  std::unique_lock<std::mutex> lock(latch_);
  auto path = path_ / name;
  if (!path.has_filename()) {
    return Status::Error(ErrorCode::DatabaseNotExists,
                         "The database now exists");
  }
  return Status::OK();
}

Status DiskManager::CreateTable(std::filesystem::path database,
                                std::string table_meta) {
  auto path = database / "meta.json";
  std::ofstream outFile(path, std::ios::binary);

  // 写入字符串内容，包括空格和换行符
  outFile.write(table_meta.data(), table_meta.size());

  // 关闭文件
  outFile.close();
  return Status::OK();
}
} // namespace DB