#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "execution/DataChunk.hpp"
#include "storage/column/Column.hpp"

#include <memory>
#include <string>
#include <unordered_map>

namespace DB {

// 全局过滤数据缓存：Filter 在 Init 阶段预先扫描并求值，把过滤后的列写入缓存，
// 子节点（ScanColumnExecutor）在 Init 时从缓存中拿过滤后的数据，避免重复扫描
struct FilteredDataCache {
  static thread_local std::unordered_map<std::string, ColumnPtr> data;
  static thread_local bool active;

  static void Set(const std::string &col_name, ColumnPtr col) {
    data[col_name] = std::move(col);
  }

  static ColumnPtr Get(const std::string &col_name) {
    auto it = data.find(col_name);
    if (it != data.end()) {
      return it->second;
    }
    return nullptr;
  }

  static void Clear() {
    data.clear();
    active = false;
  }

  static void Activate() { active = true; }
  static bool IsActive() { return active; }
};

// 向量化执行算子的抽象基类（Volcano-style 拉取模型）
//
// 生命周期：
//   1. 上层调用 Init() 一次，做预处理（扫描、计算掩码等）。
//      Init 内部会递归调用 children_->Init()，但顺序由各算子自定义
//      （例如 Filter 必须先填充缓存再 Init 子节点）
//   2. 反复调用 Next(out)，把下一个批次写入 out；out.row_count()==0 表示 EOF
//   3. 析构
//
// schema_ 描述算子的输出 schema（列名/类型）。最顶层算子的 schema 同时由
// ExecutionEngine 用作结果累加器（把所有 chunk 写回去），从而兼容
// result_set.schema_ = plan->GetSchemaRef() 的现有打印路径
class AbstractExecutor {
protected:
  SchemaRef schema_;

public:
  AbstractExecutor(SchemaRef schema) : schema_(std::move(schema)) {}

  virtual ~AbstractExecutor() = default;

  // 初始化阶段：扫描、谓词下推、构建过滤掩码等耗时操作放在这里
  virtual Status Init() = 0;

  // 拉取下一个 chunk；当 out.IsEmpty() 时表示数据流结束（EOF）
  virtual Status Next(DataChunk &out) = 0;

  SchemaRef GetSchema() { return schema_; }
};

using AbstractExecutorRef = std::unique_ptr<AbstractExecutor>;

} // namespace DB
