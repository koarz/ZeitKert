#pragma once

#include "common/Config.hpp"
#include "common/Instance.hpp"
#include "common/Status.hpp"

#include <filesystem>

namespace DB {
class DiskManager : public Instance<DiskManager> {
  std::filesystem::path path_;
  std::mutex latch_;

public:
  DiskManager() : path_(default_databases_dir) {
    std::filesystem::create_directories(path_);
  }
  
  Status CreateDatabase(std::string name);
  
  Status DropDatabase(std::string &name);
  
  Status ShowDatabase();

  Status OpenDatabase(std::string name);

  std::filesystem::path GetPath() { return path_; }
};
} // namespace DB