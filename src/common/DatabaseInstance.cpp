#include "common/DatabaseInstance.hpp"
#include "catalog/meta/TableMeta.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"

#include <memory>

namespace DB {
Database::Database(std::filesystem::path path,
                   std::shared_ptr<DiskManager> disk_manager)
    : path_(path), disk_manager_(disk_manager) {
  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    table_metas_.emplace(entry.path().filename(),
                         std::make_shared<TableMeta>(entry.path()));
  }
}

Status Database::CreateTable(
    std::string &table_name,
    std::vector<std::shared_ptr<ColumnWithNameType>> &columns) {
  table_metas_.emplace(table_name,
                       std::make_shared<TableMeta>(table_name, columns));
  return disk_manager_->CreateTable(path_ / table_name,
                                    table_metas_.rbegin()->second->Serialize());
}

Status Database::ShowTables() {
  std::vector<ResultSet::Row> result_row;
  for (const auto &entry : table_metas_) {
    result_row.emplace_back(ResultSet::Row(entry.first));
  }
  ResultSet::PrintResult(result_row, "Database");
  return Status::OK();
}
} // namespace DB