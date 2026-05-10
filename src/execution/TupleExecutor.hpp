#pragma once

#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"

#include <vector>

namespace DB {

// Tuple 算子：把多个子算子的结果横向拼成一行（典型用于 INSERT VALUES (...)）
// 在向量化模型下：Init 透传给子节点，Next 把每个子节点的当前 chunk 列拼接到 out
class TupleExecutor : public AbstractExecutor {
  std::vector<AbstractExecutorRef> children_;
  bool emitted_{false};

public:
  TupleExecutor(SchemaRef schema, std::vector<AbstractExecutorRef> children)
      : AbstractExecutor(schema), children_(std::move(children)) {}

  ~TupleExecutor() override = default;

  Status Init() override;
  Status Next(DataChunk &out) override;
};

} // namespace DB
