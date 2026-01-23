#pragma once

#include "buffer/BufferPoolManager.hpp"
#include "catalog/meta/ColumnMeta.hpp"
#include "catalog/meta/TableMeta.hpp"
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
  ~Database() {
    // save all table meta
    for (auto &[name, meta] : table_metas_) {
      std::string s = meta->Serialize();
      std::ofstream meta_file(path_ / name / TableMeta::default_table_meta_name,
                              std::ios::binary | std::ios::trunc);
      meta_file.write(s.data(), s.size());
      meta_file.close();
    }
  }

  Status CreateTable(std::string &table_name,
                     std::vector<std::shared_ptr<ColumnMeta>> &columns,
                     std::string unique_key = "");

  Status ShowTables(ResultSet &result_set);

  TableMetaRef GetTableMeta(std::string &table_name);

  std::filesystem::path GetPath() { return path_; }

  void RemoveTable(std::string name) { table_metas_.erase(name); }
};

} // namespace DB