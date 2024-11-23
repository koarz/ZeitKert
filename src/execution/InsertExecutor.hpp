#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"

namespace DB {
class InsertExecutor : public AbstractExecutor {
  TableMetaRef table_meta_;
  std::vector<AbstractExecutorRef> children_;

public:
  InsertExecutor(SchemaRef schema, std::vector<AbstractExecutorRef> children,
                 TableMetaRef table_meta)
      : AbstractExecutor(schema), children_(std::move(children)),
        table_meta_(std::move(table_meta)) {}

  ~InsertExecutor() override = default;

  // TODO:
  // insert may suffer concurrency error
  // when some insert executor insert to one column at same time
  // so we need version control to save newest data
  Status Execute() override;
};
} // namespace DB