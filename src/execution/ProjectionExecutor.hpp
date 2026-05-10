#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"

#include <vector>

namespace DB {

// 投影算子（向量化版本）
//
// Init：递归 Init 子节点。当不存在 FilteredDataCache 且子节点 ≥2 时并行 Init，
//       这样多列扫描的 IO/解码可以并发执行
// Next：依次（或并行）从每个子节点拉取一个 chunk，把列横向拼到 out
//       任一子节点 EOF 即整体 EOF
class ProjectionExecutor : public AbstractExecutor {
  std::vector<AbstractExecutorRef> children_;

public:
  ProjectionExecutor(SchemaRef schema,
                     std::vector<AbstractExecutorRef> children)
      : AbstractExecutor(schema), children_(std::move(children)) {}

  ~ProjectionExecutor() override = default;

  Status Init() override;
  Status Next(DataChunk &out) override;
};

} // namespace DB
