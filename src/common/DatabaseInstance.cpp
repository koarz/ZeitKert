#include "common/DatabaseInstance.hpp"
#include "catalog/Schema.hpp"
#include "catalog/meta/ColumnMeta.hpp"
#include "catalog/meta/TableMeta.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnWithNameType.hpp"

#include <memory>

namespace DB {
Database::Database(std::filesystem::path path,
                   std::shared_ptr<DiskManager> disk_manager,
                   std::shared_ptr<BufferPoolManager> buffer_pool_manager)
    : path_(path), disk_manager_(disk_manager) {
  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    auto [it, _] = table_metas_.emplace(
        entry.path().filename(),
        std::make_shared<TableMeta>(entry.path(), buffer_pool_manager));
  }
}

Status Database::CreateTable(std::string &table_name,
                             std::vector<std::shared_ptr<ColumnMeta>> &columns,
                             std::string unique_key) {
  table_metas_.emplace(
      table_name, std::make_shared<TableMeta>(table_name, std::move(columns),
                                              std::move(unique_key)));
  return disk_manager_->CreateTable(path_ / table_name,
                                    table_metas_.rbegin()->second->Serialize());
}

Status Database::ShowTables(ResultSet &result_set) {
  result_set.schema_ = std::make_shared<Schema>();
  auto col = std::make_shared<ColumnString>();
  for (const auto &entry : table_metas_) {
    col->Insert(std::string{entry.first});
  }
  result_set.schema_->GetColumns().push_back(
      std::make_shared<ColumnWithNameType>(col, "Tables",
                                           std::make_shared<String>()));
  return Status::OK();
}

TableMetaRef Database::GetTableMeta(std::string &table_name) {
  auto it = table_metas_.find(table_name);
  if (it == table_metas_.end()) {
    return nullptr;
  }
  return it->second;
}
} // namespace DB