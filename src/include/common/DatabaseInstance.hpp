#pragma once

#include "common/Config.hpp"
#include "common/Instance.hpp"

#include <filesystem>

namespace DB {
class Database : public Instance<Database> {
  std::filesystem::path path_;

public:
  Database() : path_(default_databases_dir) {}
  explicit Database(std::filesystem::path path) : path_(path) {}
};

} // namespace DB