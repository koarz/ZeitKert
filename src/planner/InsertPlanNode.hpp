#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/EnumClass.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "storage/lsmtree/LSMTree.hpp"

namespace DB {
class InsertPlanNode : public AbstractPlanNode {
  TableMetaRef table_meta_;
  std::shared_ptr<LSMTree> lsm_tree_;
  size_t bulk_rows_{0};

public:
  InsertPlanNode(SchemaRef schema, std::vector<AbstractPlanNodeRef> children,
                 TableMetaRef table_meta, std::shared_ptr<LSMTree> lsm_tree,
                 size_t bulk_rows = 0)
      : AbstractPlanNode(std::move(schema), std::move(children)),
        table_meta_(std::move(table_meta)), lsm_tree_(std::move(lsm_tree)),
        bulk_rows_(bulk_rows) {}
  ~InsertPlanNode() override = default;

  TableMetaRef GetTableMeta() { return table_meta_; }

  std::shared_ptr<LSMTree> GetLSMTree() { return lsm_tree_; }

  size_t GetBulkRows() const { return bulk_rows_; }

  PlanType GetType() const override { return PlanType::Insert; }
};
} // namespace DB
