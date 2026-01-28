#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "storage/lsmtree/LSMTree.hpp"

namespace DB {
class InsertExecutor : public AbstractExecutor {
  TableMetaRef table_meta_;
  std::vector<AbstractExecutorRef> children_;
  std::shared_ptr<LSMTree> lsm_tree_;
  size_t bulk_rows_{0};

public:
  InsertExecutor(SchemaRef schema, std::vector<AbstractExecutorRef> children,
                 TableMetaRef table_meta, std::shared_ptr<LSMTree> lsm_tree,
                 size_t bulk_rows = 0)
      : AbstractExecutor(schema), children_(std::move(children)),
        table_meta_(std::move(table_meta)), lsm_tree_(std::move(lsm_tree)),
        bulk_rows_(bulk_rows) {}

  ~InsertExecutor() override = default;

  // TODO:
  // insert may suffer concurrency error
  // when some insert executor insert to one column at same time
  // so we need version control to save newest data
  Status Execute() override;
};
} // namespace DB
