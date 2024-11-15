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

  Status Init() override;

  Status Execute() override;
};
} // namespace DB