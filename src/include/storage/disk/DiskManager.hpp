#pragma once

#include "common/Config.hpp"
#include "common/Instance.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"

#include <filesystem>
#include <fstream>
#include <mutex>
#include <unordered_map>

namespace DB {

class DiskManager : public Instance<DiskManager> {
  std::filesystem::path path_;
  std::mutex latch_;
  std::unordered_map<std::fstream *, std::mutex> latchs_;

public:
  DiskManager() : path_(default_databases_dir) {
    std::filesystem::create_directories(path_);
  }

  Status CreateDatabase(std::string name);

  Status DropDatabase(std::string &name);

  Status DropTable(std::filesystem::path table_path);

  Status ShowDatabase(ResultSet &result_set);

  Status OpenDatabase(std::string name);

  Status CreateTable(std::filesystem::path table, std::string table_meta);

  std::filesystem::path GetPath() { return path_; }

  Status ReadPage(std::fstream &fs, page_id_t page_id, Byte *data);

  Status WritePage(std::fstream &fs, page_id_t page_id, Byte *data);
};
} // namespace DB