#include "common/DatabaseInstance.hpp"
#include "catalog/meta/TableMeta.hpp"
#include "common/Status.hpp"

#include <memory>

namespace DB {
Status Database::CreateTable(
    std::string &table_name,
    std::vector<std::shared_ptr<ColumnWithNameType>> &columns) {
  table_metas_.emplace(table_name,
                       std::make_shared<TableMeta>(table_name, columns));
  return disk_manager_->CreateTable(path_ / table_name,
                                    table_metas_.rbegin()->second->Serialize());
}
} // namespace DB