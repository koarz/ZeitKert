#include "storage/disk/DiskManager.hpp"
#include "catalog/meta/TableMeta.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"

#include <filesystem>
#include <mutex>

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
  if (!std::filesystem::exists(path)) {
    return Status::Error(ErrorCode::CreateError, "The Database Is not Exist");
  }
  std::vector<ResultSet::Row> result_row;
  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    result_row.emplace_back(ResultSet::Row(entry.path().filename()));
  }
  ResultSet::PrintResult(result_row, "Database");
  return Status::OK();
}

Status DiskManager::OpenDatabase(std::string name) {
  auto path = path_ / name;
  if (!path.has_filename()) {
    return Status::Error(ErrorCode::DatabaseNotExists,
                         "The database not exists");
  }
  return Status::OK();
}

Status DiskManager::CreateTable(std::filesystem::path table,
                                std::string table_meta) {
  if (!std::filesystem::create_directory(table)) {
    return Status::Error(ErrorCode::CreateError, "The Table Already Exists");
  }
  auto path = table / TableMeta::default_table_meta_name;
  std::ofstream outFile(path, std::ios::binary);

  outFile.write(table_meta.data(), table_meta.size());

  outFile.close();
  return Status::OK();
}
} // namespace DB