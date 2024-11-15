#pragma once

#include "buffer/BufferPoolManager.hpp"
#include "catalog/meta/ColumnMeta.hpp"
#include "catalog/meta/TableMeta.hpp"
#include "common/Config.hpp"
#include "common/Instance.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "storage/disk/DiskManager.hpp"

#include <filesystem>
#include <map>
#include <memory>

namespace DB {
class Database : public Instance<Database> {
  std::filesystem::path path_;
  // disk manager and buffer pool just for create table meta
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_manager_;
  std::map<std::string, TableMetaRef> table_metas_;

public:
  explicit Database(std::filesystem::path path,
                    std::shared_ptr<DiskManager> disk_manager,
                    std::shared_ptr<BufferPoolManager> buffer_pool_manager);

  Status CreateTable(std::string &table_name,
                     std::vector<std::shared_ptr<ColumnMeta>> &columns);

  Status ShowTables(ResultSet &result_set);

  TableMetaRef GetTableMeta(std::string &table_name);
};

} // namespace DB