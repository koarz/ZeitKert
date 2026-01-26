#include "storage/disk/DiskManager.hpp"
#include "catalog/meta/TableMeta.hpp"
#include "common/Config.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "storage/column/ColumnString.hpp"

#include <cstring>
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
    if (!std::filesystem::remove_all(path)) {
      return Status::Error(ErrorCode::DropError,
                           "The Database can't be dropped");
    }
  } catch (const std::filesystem::filesystem_error &e) {
    return Status::Error(ErrorCode::DropError,
                         std::format("Error drop database: {}", e.what()));
  }
  return Status::OK();
}

Status DiskManager::DropTable(std::filesystem::path table_path) {
  try {
    if (!std::filesystem::remove_all(table_path)) {
      return Status::Error(ErrorCode::DropError, "The Table can't be dropped");
    }
  } catch (const std::filesystem::filesystem_error &e) {
    return Status::Error(ErrorCode::DropError,
                         std::format("Error drop table: {}", e.what()));
  }
  return Status::OK();
}

Status DiskManager::ShowDatabase(ResultSet &result_set) {
  auto path = path_;
  if (!std::filesystem::exists(path)) {
    return Status::Error(ErrorCode::CreateError, "The Database dose not Exist");
  }
  result_set.schema_ = std::make_shared<Schema>();
  auto col = std::make_shared<ColumnString>();
  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    col->Insert(std::string{entry.path().filename()});
  }
  result_set.schema_->GetColumns().push_back(
      std::make_shared<ColumnWithNameType>(col, "Databases",
                                           std::make_shared<String>()));
  return Status::OK();
}

Status DiskManager::OpenDatabase(std::string name) {
  auto path = path_ / name;
  if (!std::filesystem::is_directory(path)) {
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
  std::ofstream meta_file(path, std::ios::binary);

  meta_file.write(table_meta.data(), table_meta.size());

  meta_file.close();
  return Status::OK();
}

Status DiskManager::ReadPage(std::fstream &fs, page_id_t page_id, Byte *data) {
  latch_.lock();
  std::unique_lock latch(latchs_[&fs]);
  latch_.unlock();
  if (!fs.is_open()) {
    return Status::Error(ErrorCode::FileNotOpen, "File not open");
  }
  auto offset = static_cast<std::streamoff>(page_id) * DEFAULT_PAGE_SIZE;
  fs.clear();
  fs.seekg(offset, std::ios::beg);
  if (fs.fail()) {
    return Status::Error(ErrorCode::IOError, "I/O error when seeking page");
  }
  fs.read(reinterpret_cast<char *>(data), DEFAULT_PAGE_SIZE);
  if (fs.bad()) {
    return Status::Error(ErrorCode::IOError, "I/O error when reading page");
  }
  int read_count = fs.gcount();
  if (read_count < DEFAULT_PAGE_SIZE) {
    fs.clear();
    memset(data + read_count, 0, DEFAULT_PAGE_SIZE - read_count);
  }
  return Status::OK();
}

Status DiskManager::WritePage(std::fstream &fs, page_id_t page_id, Byte *data) {
  latch_.lock();
  std::unique_lock latch(latchs_[&fs]);
  latch_.unlock();
  if (!fs.is_open()) {
    return Status::Error(ErrorCode::FileNotOpen, "File not open");
  }
  auto offset = static_cast<std::streamoff>(page_id) * DEFAULT_PAGE_SIZE;
  fs.clear();
  fs.seekp(offset, std::ios::beg);
  if (fs.fail()) {
    return Status::Error(ErrorCode::IOError, "I/O error when seeking page");
  }
  fs.write(reinterpret_cast<char *>(data), DEFAULT_PAGE_SIZE);
  if (fs.fail()) {
    return Status::Error(ErrorCode::IOError, "I/O error when writing page");
  }
  fs.flush();
  return Status::OK();
}
} // namespace DB
