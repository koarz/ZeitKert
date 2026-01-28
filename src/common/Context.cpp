#include "common/Context.hpp"

#include "storage/lsmtree/LSMTree.hpp"

namespace DB {
std::shared_ptr<LSMTree>
QueryContext::GetOrCreateLSMTree(const TableMetaRef &table_meta) {
  if (!table_meta) {
    return nullptr;
  }
  auto name = table_meta->GetTableName();
  auto it = lsm_trees_.find(name);
  if (it != lsm_trees_.end()) {
    return it->second;
  }
  auto types = table_meta->GetColumnTypes();
  int pk_idx = table_meta->GetPrimaryKeyIndex();
  uint16_t primary_key = pk_idx < 0 ? 0 : static_cast<uint16_t>(pk_idx);
  auto lsm = std::make_shared<LSMTree>(table_meta->GetTablePath(),
                                       buffer_pool_manager_, std::move(types),
                                       primary_key);
  lsm_trees_.emplace(name, lsm);
  return lsm;
}
} // namespace DB
