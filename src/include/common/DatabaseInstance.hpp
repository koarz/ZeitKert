#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/Config.hpp"
#include "common/Instance.hpp"
#include "common/Status.hpp"
#include "storage/disk/DiskManager.hpp"

#include <filesystem>
#include <map>
#include <memory>

namespace DB {
class Database : public Instance<Database> {
  std::filesystem::path path_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::map<std::string, std::shared_ptr<TableMeta>> table_metas_;

public:
  explicit Database(std::filesystem::path path,
                    std::shared_ptr<DiskManager> disk_manager);

  Status CreateTable(std::string &table_name,
                     std::vector<std::shared_ptr<ColumnWithNameType>> &columns);

  Status ShowTables();
};

} // namespace DB