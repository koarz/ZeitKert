#pragma once

#include "catalog/meta/ColumnMeta.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "storage/lsmtree/LSMTree.hpp"

namespace DB {
class ScanColumnPlanNode : public AbstractPlanNode {
  ColumnMetaRef column_meta_;
  std::shared_ptr<LSMTree> lsm_tree_;
  uint32_t column_idx_{0};

public:
  ScanColumnPlanNode(SchemaRef schema, ColumnMetaRef column_meta,
                     std::shared_ptr<LSMTree> lsm_tree, uint32_t column_idx)
      : AbstractPlanNode(std::move(schema), {}),
        column_meta_(std::move(column_meta)), lsm_tree_(std::move(lsm_tree)),
        column_idx_(column_idx) {}
  ~ScanColumnPlanNode() override = default;

  ColumnMetaRef &GetColumnMeta() { return column_meta_; }

  std::shared_ptr<LSMTree> GetLSMTree() { return lsm_tree_; }

  uint32_t GetColumnIndex() const { return column_idx_; }

  PlanType GetType() const override { return PlanType::SeqScan; }
};
} // namespace DB
